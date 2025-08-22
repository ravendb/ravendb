import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import EditAiAgent from "./EditAiAgent";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/AI Hub/AI Agents/Edit AI Agent",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const EditAiAgentStory: StoryObj = {
    name: "Edit AI Agent",
    render: () => {
        mockStore.databases.withActiveDatabase();

        return <EditAiAgent />;
    },
};
