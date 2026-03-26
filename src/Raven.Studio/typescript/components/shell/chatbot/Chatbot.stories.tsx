import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import Chatbot from "./Chatbot";
import { mockStore } from "test/mocks/store/MockStore";
import { ChatbotStubs } from "test/stubs/ChatbotStubs";
import { ChatbotUserActionState } from "components/shell/chatbot/store/chatbotSlice";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";

export default {
    title: "Shell/Chatbot",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function commonInit() {
    const { chatbot, aiAssistant, cluster, license } = mockStore;

    license.with_License();
    cluster.with_ClientVersion("7.2");
    cluster.with_ServerVersion();
    aiAssistant.with_consent("Success");
    chatbot.with_isOpen(true);
    chatbot.with_isAlwaysAllowEndpointCalls(false);
    chatbot.with_attachedContextUpserted({
        id: "View",
        type: "View",
        label: "Edit Index",
        value: "Edit Index",
        state: "included",
    });
    chatbot.with_attachedContextUpserted({
        id: "DatabaseName",
        type: "DatabaseName",
        label: "Sample DB",
        value: "Sample DB",
        state: "included",
    });
    chatbot.with_attachedContextUpserted({
        id: "DocumentId",
        type: "DocumentId",
        label: "products/78-A",
        value: '{"Name":"SomeName","Supplier":"suppliers/23-A","Category":"categories/1-A","QuantityPerUnit":"500 ml"',
        state: "included",
    });
    chatbot.with_attachedContextUpserted({
        id: "IndexName",
        type: "IndexName",
        label: "Auto/Categories/ById()",
        value: '{"Name":"Auto/Categories/ById()","SourceType":"Documents","Type":"AutoMap"',
        state: "included",
    });
}

function ChatbotInLayout() {
    return (
        <div className="layout-container show-chatbot pin-chatbot">
            <Chatbot />
        </div>
    );
}

export const Basic: StoryObj = {
    render: () => {
        commonInit();

        const { chatbot } = mockStore;
        chatbot.with_messages(ChatbotStubs.basicMessages());

        return <ChatbotInLayout />;
    },
};

export const Endpoints: StoryObj<{ actionState: ChatbotUserActionState }> = {
    render: (args) => {
        commonInit();

        const { chatbot } = mockStore;
        chatbot.with_messages(ChatbotStubs.messagesWithEndpoints(args.actionState));

        return <ChatbotInLayout />;
    },
    args: {
        actionState: "waiting",
    },
    argTypes: {
        actionState: {
            control: { type: "radio" },
            options: [
                "waiting",
                "allowed",
                "alwaysAllowed",
                "skipped",
                "denied",
                "error",
            ] satisfies ChatbotUserActionState[],
        },
    },
};

export const Empty: StoryObj = {
    render: () => {
        commonInit();

        return <ChatbotInLayout />;
    },
};

export const NoConsent: StoryObj<{ consent: CheckConsentAiAssistantResultDto["Status"] }> = {
    render: (args) => {
        const { chatbot, aiAssistant, license, cluster } = mockStore;

        license.with_License();
        cluster.with_ClientVersion("7.2");
        aiAssistant.with_consent(args.consent);
        chatbot.with_isOpen(true);

        return <ChatbotInLayout />;
    },
    args: {
        consent: "ConsentRequired",
    },
    argTypes: {
        consent: {
            control: { type: "radio" },
            options: ["ConsentRequired", "InvalidCredentials"] satisfies CheckConsentAiAssistantResultDto["Status"][],
        },
    },
};
