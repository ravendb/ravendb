import { ChatbotMessage } from "components/shell/chatbot/store/chatbotSlice";

export class ChatbotStubs {
    static basicMessages(): ChatbotMessage[] {
        return [
            {
                id: "1",
                role: "user",
                content: "Explain this view",
                attachedContexts: [
                    {
                        id: "currentView",
                        type: "Current View",
                        label: "All Documents",
                        value: "All Documents",
                        state: "included",
                    },
                    {
                        id: "currentDatabaseName",
                        type: "Current Database Name",
                        label: "sample",
                        value: "sample",
                        state: "included",
                    },
                ],
            },
            {
                id: "2",
                role: "assistant",
                content:
                    'In RavenDB Studio, the "All Documents" view presents a comprehensive list of all documents within the selected database, which in your case is named "sample." This view allows you to browse, search, and manage every document stored in that database. Essentially, it is where you see the entirety of your data held in the database in a document-centric format, giving you the ability to inspect and perform operations on individual documents.',
                state: "Success",
                thinkingTimeInMs: 15061,
                usage: null,
                relevantLinks: [
                    {
                        Url: "https://docs.ravendb.net/7.1/studio/overview/",
                        Title: "Studio Overview",
                    },
                ],
                followUpQuestions: [
                    "How can I query documents in the All Documents view?",
                    "How do I create and manage indexes in RavenDB Studio?",
                    "How can I import and export data using RavenDB Studio?",
                ],
                endpoints: [],
                additionalContext: {},
                userActionState: null,
            },
        ];
    }

    static messagesWithAdditionalContext(): ChatbotMessage[] {
        return [
            {
                id: "1",
                role: "user",
                content: "How to query all documents in collection?",
                attachedContexts: [
                    {
                        id: "currentView",
                        type: "Current View",
                        label: "All Documents",
                        value: "All Documents",
                        state: "included",
                    },
                    {
                        id: "currentDatabaseName",
                        type: "Current Database Name",
                        label: "sample",
                        value: "sample",
                        state: "included",
                    },
                ],
            },
            {
                id: "2",
                role: "assistant",
                content: "",
                state: "Success",
                thinkingTimeInMs: 1505,
                usage: null,
                endpoints: [],
                additionalContext: {
                    call_3qQEMACL5cB1zkSMVTkWRsu1: {
                        Message: "Please specify the collection name.",
                        Option: "CollectionName",
                    },
                    call_3qQEMACL5cB1zkSMVTkWRsu2: {
                        Message: "Please specify the database name.",
                        Option: "DatabaseName",
                    },
                    call_3qQEMACL5cB1zkSMVTkWRsu3: {
                        Message: "Please specify the document ID.",
                        Option: "DocumentId",
                    },
                    call_3qQEMACL5cB1zkSMVTkWRsu4: {
                        Message: "Please specify the Index name.",
                        Option: "IndexName",
                    },
                },
                userActionState: "waiting",
                followUpQuestions: [],
                relevantLinks: [],
            },
        ];
    }

    static messagesWithEndpoints(): ChatbotMessage[] {
        return [
            {
                id: "1",
                role: "user",
                content: "How to query all documents in collection?",
                attachedContexts: [
                    {
                        id: "currentView",
                        type: "Current View",
                        label: "All Documents",
                        value: "All Documents",
                        state: "included",
                    },
                    {
                        id: "currentDatabaseName",
                        type: "Current Database Name",
                        label: "sample",
                        value: "sample",
                        state: "included",
                    },
                ],
            },
            {
                id: "2",
                role: "assistant",
                content: "",
                state: "Success",
                thinkingTimeInMs: 3018,
                usage: null,
                endpoints: [
                    {
                        toolId: "call_180MLXGBoOd1yMC79Lq7M1Qt",
                        url: "/databases/AiAssistant/indexes?pageSize=1024",
                        state: "waiting",
                    },
                    {
                        toolId: "call_wrdx4K9jyZhDjbZaQcZAS047",
                        url: "/databases/AiAssistant/indexes/stats?pageSize=1024",
                        state: "waiting",
                    },
                ],
                userActionState: "waiting",
                additionalContext: {},
                followUpQuestions: [],
                relevantLinks: [],
            },
        ];
    }
}
