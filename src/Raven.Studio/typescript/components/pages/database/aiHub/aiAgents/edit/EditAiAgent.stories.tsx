import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import EditAiAgent from "./EditAiAgent";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/AI Hub/AI Agents/Edit AI Agent",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const EditAiAgentStory: StoryObj = {
    name: "Edit AI Agent",
    render: () => {
        const { databases } = mockStore;
        const { aiAssistantService } = mockServices;

        databases.withActiveDatabase();

        aiAssistantService.withCheckConsent();
        aiAssistantService.withAssist();

        return <EditAiAgent />;
    },
};
