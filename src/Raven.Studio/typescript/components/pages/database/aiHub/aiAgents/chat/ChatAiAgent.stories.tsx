import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ChatAiAgent from "./ChatAiAgent";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { AiAgentStubs } from "test/stubs/AiAgentStubs";
import document from "models/database/documents/document";

export default {
    title: "Pages/AI Hub/AI Agents/Chat AI Agent",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface ChatAiAgentStoryArgs {
    agentId: string;
    conversationId: string;
    isHistory: boolean;
    agents: GetAiAgentResultDto;
    conversationDocument: document;
}

export const ChatAiAgentStory: StoryObj<ChatAiAgentStoryArgs> = {
    name: "Chat AI Agent",
    render: (args) => {
        const { databases } = mockStore;
        const { aiAgentService, databasesService } = mockServices;

        databases.withActiveDatabase();

        aiAgentService.withAiAgents(args.agents);
        databasesService.withDocumentWithMetadata(args.conversationDocument);

        return (
            <div style={{ height: 1000 }}>
                <ChatAiAgent
                    queryParams={{
                        agentId: args.agentId,
                        conversationId: args.conversationId,
                        isHistory: args.isHistory,
                    }}
                />
            </div>
        );
    },
    args: {
        agentId: "first-agent",
        conversationId: "conversation-1",
        isHistory: false,
        agents: AiAgentStubs.getAiAgents(),
        conversationDocument: AiAgentStubs.getAiAgentDocument(),
    },
};
