import { Plugin, ProcessedPluginEvent } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMetaInput {
    config: {
        host: string
        project_api_key: string
        replication: string
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
        const batch = []
        for (const event of events) {
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
            await fetch(`https://${config.host.replace(/\/$/, "")}/e`, {
                method: 'POST',
                body: JSON.stringify(batch),
                headers: { 'Content-Type': 'application/json' },
            })
            console.log(`Flushing ${batch.length} event${batch.length > 1 ? 's' : ''} to ${config.host}`)
        }
    },
}

module.exports = plugin
