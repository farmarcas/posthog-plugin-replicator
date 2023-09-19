import { Plugin, ProcessedPluginEvent, RetryError } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMetaInput {
    config: {
        host: string
        project_api_key: string
        replication: string
        events_to_ignore: string
    }
}

type StrippedEvent = Omit<ProcessedPluginEvent, 'team_id' | 'ip' | 'person'>

const reverseAutocaptureEvent = (autocaptureEvent: StrippedEvent) => {
    // TRICKY: This code basically reverses what the plugin server does
    // Adapted from https://github.com/PostHog/posthog/blob/master/plugin-server/src/utils/db/elements-chain.ts#L105
    const { elements, properties, ...event } = autocaptureEvent

    const $elements = elements?.map((el) => {
        // $el_text and attributes are the only differently named parts
        const { attributes, text, ...commonProps } = el
        return {
            ...commonProps,
            $el_text: text,
            ...attributes,
        }
    })

    return {
        ...event,
        properties: $elements
            ? {
                  ...properties,
                  $elements,
              }
            : properties,
    }
}

const plugin: Plugin<ReplicatorMetaInput> = {
    exportEvents: async (events, { config }) => {
        const eventsToIgnore = new Set(
            config.events_to_ignore && config.events_to_ignore.trim() !== ''
                ? config.events_to_ignore.split(',').map((event) => event.trim())
                : null
        )
        const batch = []
        for (const event of events) {
            // skip if event has to be ignored
            if (eventsToIgnore.has(event.event)) {
                continue
            }

            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { team_id, ip, person: _, ...sendableEvent } = { ...event, token: config.project_api_key }

            if (ip) {
                // Set IP address (originally obtained from capture request headers) in properties
                sendableEvent.properties.$ip = ip
            }

            const finalSendableEvent =
                sendableEvent.event === '$autocapture' ? reverseAutocaptureEvent(sendableEvent) : sendableEvent

            const replication = parseInt(config.replication) || 1
            for (let i = 0; i < replication; i++) {
                batch.push(finalSendableEvent)
            }
        }

        if (batch.length > 0) {
            const batchDescription = `${batch.length} event${batch.length > 1 ? 's' : ''}`

            await fetch(`${config.host.replace(/\/$/, '')}/e`, {
                method: 'POST',
                body: JSON.stringify(batch),
                headers: { 'Content-Type': 'application/json' },
                // TODO: add a timeout signal to make sure we retry if capture is slow, instead of failing the export
            }).then(
                (res) => {
                    if (res.ok) {
                        console.log(`Flushed ${batchDescription} to ${config.host}`)
                    } else if (res.status >= 500) {
                        // Server error, retry the batch later
                        console.error(
                            `Failed to submit ${batchDescription} to ${config.host} due to server error: ${res.status} ${res.statusText}`
                        )
                        throw new RetryError(`Server error: ${res.status} ${res.statusText}`)
                    } else {
                        // node-fetch handles 300s internaly, so we're left with 400s here: skip the batch and move forward
                        // We might have old events in ClickHouse that don't pass new stricter checks, don't fail the whole export if that happens
                        console.warn(
                            `Skipping ${batchDescription}, rejected by ${config.host}: ${res.status} ${res.statusText}`
                        )
                    }
                },
                (err) => {
                    if (err.name === 'AbortError' || err.name === 'FetchError') {
                        // Network / timeout error, retry the batch later
                        // See https://github.com/node-fetch/node-fetch/blob/2.x/ERROR-HANDLING.md
                        console.error(
                            `Failed to submit ${batchDescription} to ${config.host} due to network error`,
                            err
                        )
                        throw new RetryError(`Target is unreachable: ${(err as Error).message}`)
                    }
                    // Other errors are rethrown to stop the export
                    console.error(`Failed to submit ${batchDescription} to ${config.host} due to unexpected error`, err)
                    throw err
                }
            )
        }
    },
}

module.exports = plugin
