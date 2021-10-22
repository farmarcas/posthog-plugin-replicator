import { Plugin } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMetaInput {
    config: {
        host: string
        project_api_key: string
        replication: string
    }
}

const plugin: Plugin<ReplicatorMetaInput> = {
    exportEvents: async (events, { config }) => {
        const batch = []
        for (const event of events) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { team_id, now, offset, sent_at, $token, project_id, api_key, ...sendableEvent } = { ...event, token: config.project_api_key }
            const replication = parseInt(config.replication) || 1
            for (let i = 0; i < replication; i++) {
                batch.push(sendableEvent)
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
