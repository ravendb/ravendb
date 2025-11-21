import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import Chatbot from "./Chatbot";
import { mockStore } from "test/mocks/store/MockStore";
import { ChatbotStubs } from "test/stubs/ChatbotStubs";

export default {
    title: "Shell/Chatbot",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function commonInit() {
    const { chatbot, aiAssistant } = mockStore;

    aiAssistant.with_consent("Success");
    chatbot.with_isOpen(true);
    chatbot.with_attachedContextSet({ id: "currentView", label: "Edit Index", value: "Edit Index" });
    chatbot.with_attachedContextSet({ id: "currentDatabaseName", label: "Sample DB", value: "Sample DB" });
    chatbot.with_attachedContextSet({
        id: "currentDocument",
        label: "products/78-A",
        value: '{"Name":"SomeName","Supplier":"suppliers/23-A","Category":"categories/1-A","QuantityPerUnit":"500 ml"',
    });
    chatbot.with_attachedContextSet({
        id: "currentIndexDefinition",
        label: "Auto/Categories/ById()",
        value: '{"Name":"Auto/Categories/ById()","SourceType":"Documents","Type":"AutoMap"',
    });
}

function ChatbotInLayout() {
    return (
        <div className="layout-container show-chatbot pin-chatbot">
            <Chatbot />
        </div>
    );
}

export const Basic: StoryObj<typeof Chatbot> = {
    render: () => {
        commonInit();

        const { chatbot } = mockStore;
        chatbot.with_messages(ChatbotStubs.basicMessages());

        return <ChatbotInLayout />;
    },
};

export const AdditionalContext: StoryObj<typeof Chatbot> = {
    render: () => {
        commonInit();

        const { chatbot } = mockStore;
        chatbot.with_messages(ChatbotStubs.messagesWithAdditionalContext());

        return <ChatbotInLayout />;
    },
};

export const Endpoints: StoryObj<typeof Chatbot> = {
    render: () => {
        commonInit();

        const { chatbot } = mockStore;
        chatbot.with_messages(ChatbotStubs.messagesWithEndpoints());

        return <ChatbotInLayout />;
    },
};
