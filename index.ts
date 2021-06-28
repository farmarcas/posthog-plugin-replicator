import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMeta extends PluginMeta {
    global: {
        buffer: ReturnType<typeof createBuffer>
    }
    config: {
        host: string
        project_api_key: string
        replication: string
    }
}

const plugin: Plugin<ReplicatorMeta> = {
    setupPlugin: ({ global, config }) => {
        global.buffer = createBuffer({
            limit: 1024 * 1024, // 1 MB
            timeoutSeconds: 1,
            onFlush: async (batch) => {
                await fetch(`https://${config.host}/e`, {
                    method: 'POST',
                    body: JSON.stringify(batch),
                    headers: { 'Content-Type': 'application/json' },
                })
                console.log(`Flushing ${batch.length} event${batch.length > 1 ? 's' : ''} to ${config.host}`)
            },
        })
    },

    teardownPlugin: ({ global }) => {
        global.buffer.flush()
    },

    processEvent: async (event, { config, global }) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { team_id, now, offset, ...sendableEvent } = { ...event, token: config.project_api_key }
        const sendableEventSize = JSON.stringify(sendableEvent).length
        const replication = parseInt(config.replication) || 1
        for (let i = 0; i < replication; i++) {
            global.buffer.add(sendableEvent, sendableEventSize)
        }
        return event
    },
}

module.exports = plugin
