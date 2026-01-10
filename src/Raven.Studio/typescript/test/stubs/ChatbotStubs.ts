import { ChatbotMessage, ChatbotUserActionState } from "components/shell/chatbot/store/chatbotSlice";

export class ChatbotStubs {
    static basicMessages(): ChatbotMessage[] {
        return [
            {
                id: "1",
                role: "user",
                content: "Explain this view",
                attachedContexts: [
                    {
                        id: "View",
                        type: "View",
                        label: "All Documents",
                        value: "All Documents",
                        state: "included",
                    },
                    {
                        id: "DatabaseName",
                        type: "DatabaseName",
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
                userActionState: null,
            },
        ];
    }

    static messagesWithEndpoints(actionState: ChatbotUserActionState = "waiting"): ChatbotMessage[] {
        const isWithSize = actionState === "allowed" || actionState === "alwaysAllowed";

        return [
            {
                id: "1",
                role: "user",
                content: "How to query all documents in collection?",
                attachedContexts: [
                    {
                        id: "View",
                        type: "View",
                        label: "All Documents",
                        value: "All Documents",
                        state: "included",
                    },
                    {
                        id: "DatabaseName",
                        type: "DatabaseName",
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
                        state: actionState,
                        resultSizeInBytes: isWithSize ? 123 : undefined,
                    },
                    {
                        toolId: "call_wrdx4K9jyZhDjbZaQcZAS047",
                        url: "/databases/test/docs?id=orders%2F830-A&id=orders%2F831313131313131313131313131313131313131313131313131313131313131-A",
                        state: actionState,
                        resultSizeInBytes: isWithSize ? 12_000 : undefined,
                        isRequestTooLarge: true,
                    },
                ],
                userActionState: actionState,
                followUpQuestions: [],
                relevantLinks: [],
            },
        ];
    }
}
