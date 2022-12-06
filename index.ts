import { Plugin } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMetaInput {
    config: {
        host: string
        project_api_key: string
        replication: string
    }
}

const reverseAutocaptureEvent = (autocaptureEvent: any) => {
    // TRICKY: This code basically reverses what the plugin server does
    // Adapted from https://github.com/PostHog/posthog/blob/master/plugin-server/src/utils/db/elements-chain.ts#L105
    const { elements, properties, ip, person: _, ...event } = autocaptureEvent

    const $elements = elements.map((el: any) => {
        // $el_text and attributes are the only differently named parts
        const { attributes, text, ...commonProps } = el
        return {
            ...commonProps,
            $ip: ip,
            $el_text: text,
            ...attributes,
        }
    })

    return {
        ...event,
        properties: {
            ...properties,
            $elements: $elements,
        },
    }
}

const plugin: Plugin<ReplicatorMetaInput> = {
    exportEvents: async (events, { config }) => {
        const batch = []
        for (const event of events) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { team_id, ...sendableEvent } = { ...event, token: config.project_api_key }
            const replication = parseInt(config.replication) || 1
            const eventToSend =
                sendableEvent.event === '$autocapture' ? reverseAutocaptureEvent(sendableEvent) : sendableEvent

            for (let i = 0; i < replication; i++) {
                batch.push(eventToSend)
            }
        }

        if (batch.length > 0) {
            await fetch(`https://${config.host}/e`, {
                method: 'POST',
                body: JSON.stringify(batch),
                headers: { 'Content-Type': 'application/json' },
            })
            console.log(`Flushing ${batch.length} event${batch.length > 1 ? 's' : ''} to ${config.host}`)
        }
    },
}

module.exports = plugin
