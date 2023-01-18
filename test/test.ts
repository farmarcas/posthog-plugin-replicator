jest.mock('node-fetch')
import fetch from 'node-fetch'

const plugin = require('../index')
const config = {
    host: 'localhost:8000',
    project_api_key: 'test',
    replication: 1,
}

const mockEvent = require('./data/event.json')
const mockAutocaptureEvent = require('./data/autocapture-event.json')

describe('event pre-processing', () => {
    const mockFetch = fetch as jest.MockedFunction<typeof fetch>
    mockFetch.mockResolvedValue({ ok: true } as any)

    beforeEach(() => {
        mockFetch.mockClear()
    })

    describe('it replicates events', () => {
        it('should correctly reverse the autocapture format', async () => {
            await plugin.exportEvents([mockEvent, mockEvent, mockEvent], { config })
            expect(mockFetch.mock.calls.length).toEqual(1)
            expect(mockFetch.mock.calls[0]).toEqual([
                'https://localhost:8000/e',
                {
                    body: JSON.stringify([
                        {
                            distinct_id: '1234',
                            event: 'my-event',
                            properties: { foo: 'bar', $ip: '127.0.0.1' },
                            token: 'test',
                        },
                        {
                            distinct_id: '1234',
                            event: 'my-event',
                            properties: { foo: 'bar', $ip: '127.0.0.1' },
                            token: 'test',
                        },
                        {
                            distinct_id: '1234',
                            event: 'my-event',
                            properties: { foo: 'bar', $ip: '127.0.0.1' },
                            token: 'test',
                        },
                    ]),
                    headers: { 'Content-Type': 'application/json' },
                    method: 'POST',
                },
            ])
        })

        it('should reuse the values for timestamp, event, uuid', async () => {
            // This is important to ensure that we end up sending the correct
            // values for the properties that we dedup.
            // NOTE: we should be reasonability confident that these are the
            // values we'd recieve as per the functional test here:
            // https://github.com/PostHog/posthog/blob/771691e8bdd6bf4465887b88d0a6019c9b4b91d6/plugin-server/functional_tests/exports-v2.test.ts#L151

            await plugin.exportEvents(
                [{ distinct_id: '1234', event: 'my-event', sent_at: 'asdf', uuid: 'asdf-zxcv' }],
                { config }
            )

            expect(mockFetch.mock.calls[0]).toEqual([
                'https://localhost:8000/e',
                {
                    body: JSON.stringify([
                        {
                            distinct_id: '1234',
                            event: 'my-event',
                            sent_at: 'asdf',
                            uuid: 'asdf-zxcv',
                            token: 'test',
                        },
                    ]),
                    headers: { 'Content-Type': 'application/json' },
                    method: 'POST',
                },
            ])
        })
    })

    describe('autocapture support', () => {
        it('should correctly reverse the autocapture format', async () => {
            await plugin.exportEvents([mockAutocaptureEvent], { config })

            const parsedBody = JSON.parse(mockFetch.mock.calls[0][1]?.body as string)

            expect(parsedBody[0]).toEqual({
                distinct_id: '12345',
                event: '$autocapture',

                timestamp: '2022-12-06T12:54:30.810Z',
                token: 'test',
                uuid: '0184e780-8f2b-0000-9e00-4cdcba315fe9',
                properties: {
                    $browser: 'Firefox',
                    $os: 'Mac OS X',
                    $ip: '127.0.0.1',
                    $elements: [
                        {
                            order: 0,
                            tag_name: 'div',
                            attr_class: 'view-line',
                            nth_child: 18,
                            nth_of_type: 18,
                            attr__class: 'view-line',
                            attr__style: 'top:396px;height:18px;',
                        },
                        {
                            order: 1,
                            tag_name: 'div',
                            attr_class: 'view-lines monaco-mouse-cursor-text',
                            nth_child: 4,
                            nth_of_type: 4,
                            'attr__aria-hidden': 'true',
                            attr__class: 'view-lines monaco-mouse-cursor-text',
                            'attr__data-mprt': '7',
                            attr__role: 'presentation',
                            attr__style:
                                'position: absolute; font-family: Menlo, Monaco, \\"Courier New\\", monospace; font-weight: normal; font-size: 12px; font-feature-settings: \\"liga\\" 0, \\"calt\\" 0; line-height: 18px; letter-spacing: 0px; width: 899px; height: 1438px;',
                        },
                        {
                            order: 2,
                            tag_name: 'div',
                            attr_class: 'lines-content monaco-editor-background',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'lines-content monaco-editor-background',
                            attr__style:
                                'position: absolute; overflow: hidden; width: 1000000px; height: 1000000px; transform: translate3d(0px, 0px, 0px); contain: strict; top: -93px; left: 0px;',
                        },
                        {
                            order: 3,
                            tag_name: 'div',
                            attr_class: 'monaco-scrollable-element editor-scrollable vs-dark mac',
                            nth_child: 2,
                            nth_of_type: 2,
                            attr__class: 'monaco-scrollable-element editor-scrollable vs-dark mac',
                            'attr__data-mprt': '5',
                            attr__role: 'presentation',
                            attr__style:
                                'position: absolute; overflow: hidden; left: 62px; width: 899px; height: 700px;',
                        },
                        {
                            order: 4,
                            tag_name: 'div',
                            attr_class: 'overflow-guard',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'overflow-guard',
                            'attr__data-mprt': '3',
                            attr__style: 'width: 961px; height: 700px;',
                        },
                        {
                            order: 5,
                            tag_name: 'div',
                            attr_class: 'monaco-editor no-user-select mac  showUnused showDeprecated vs-dark focused',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'monaco-editor no-user-select mac  showUnused showDeprecated vs-dark focused',
                            'attr__data-uri': 'file:///index.ts',
                            attr__role: 'code',
                            attr__style: 'width: 961px; height: 700px;',
                        },
                        {
                            order: 6,
                            tag_name: 'div',
                            nth_child: 1,
                            nth_of_type: 1,
                            'attr__data-keybinding-context': '2',
                            'attr__data-mode-id': 'typescript',
                            attr__style: 'width: 100%;',
                        },
                        {
                            order: 7,
                            tag_name: 'section',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__style:
                                'display: flex; position: relative; text-align: initial; width: 100%; height: 700px;',
                        },
                        {
                            order: 8,
                            tag_name: 'div',
                            attr_class: 'Field flex gap-2 flex-col',
                            nth_child: 3,
                            nth_of_type: 2,
                            attr__class: 'Field flex gap-2 flex-col',
                        },
                        {
                            order: 9,
                            tag_name: 'form',
                            attr_class: 'PluginSource',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'PluginSource',
                        },
                        {
                            order: 10,
                            tag_name: 'div',
                            attr_class: 'ant-drawer-body',
                            nth_child: 2,
                            nth_of_type: 2,
                            attr__class: 'ant-drawer-body',
                        },
                        {
                            order: 11,
                            tag_name: 'div',
                            attr_class: 'ant-drawer-wrapper-body',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'ant-drawer-wrapper-body',
                        },
                        {
                            order: 12,
                            tag_name: 'div',
                            attr_class: 'ant-drawer-content',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'ant-drawer-content',
                        },
                        {
                            order: 13,
                            tag_name: 'div',
                            attr_class: 'ant-drawer-content-wrapper',
                            nth_child: 2,
                            nth_of_type: 2,
                            attr__class: 'ant-drawer-content-wrapper',
                            attr__style: 'width: min(90vw, 64rem);',
                        },
                        {
                            order: 14,
                            tag_name: 'div',
                            attr_class: 'ant-drawer ant-drawer-left ant-drawer-open',
                            nth_child: 1,
                            nth_of_type: 1,
                            attr__class: 'ant-drawer ant-drawer-left ant-drawer-open',
                            attr__style: 'z-index: 950;',
                            attr__tabindex: '-1',
                        },
                        { order: 15, tag_name: 'div', nth_child: 12, nth_of_type: 7 },
                        {
                            order: 16,
                            tag_name: 'body',
                            attr_class: 'ant-scrolling-effect',
                            nth_child: 7,
                            nth_of_type: 1,
                            attr__class: 'ant-scrolling-effect',
                            attr__style: 'touch-action: none; width: calc(100% - 15px); overflow: hidden;',
                        },
                    ],
                },
            })
        })
    })
})
