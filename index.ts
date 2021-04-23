import { Plugin, PluginMeta } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

export interface ReplicatorMeta extends PluginMeta {
    config: {
        host: string
        project_api_key: string
        replication: string
    }
}

const plugin: Plugin<ReplicatorMeta> = {
    processEvent: async (event, { config }) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { team_id, now, offset, ...sendableEvent } = { ...event, token: config.project_api_key }
        await fetch(`https://${config.host}/e`, {
            method: 'POST',
            body: JSON.stringify(Array(parseInt(config.replication) || 1).fill(sendableEvent)),
            headers: { 'Content-Type': 'application/json' },
        })
        return event
    },
}

module.exports = plugin
