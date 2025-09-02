import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ChatAiAgent from "./ChatAiAgent";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { AiAgentStubs } from "test/stubs/AiAgentStubs";

export default {
    title: "Pages/AI Hub/AI Agents/Chat AI Agent",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const ChatAiAgentStory: StoryObj = {
    name: "Chat AI Agent",
    render: () => {
        const { databases } = mockStore;
        const { aiAgentService, databasesService } = mockServices;

        databases.withActiveDatabase();

        aiAgentService.withAiAgents();
        databasesService.withDocumentWithMetadata(AiAgentStubs.getAiAgentDocument());

        return (
            <div style={{ height: 700 }}>
                <ChatAiAgent
                    queryParams={{ agentId: "first-agent", conversationId: "conversation-1", isHistory: false }}
                />
            </div>
        );
    },
};
