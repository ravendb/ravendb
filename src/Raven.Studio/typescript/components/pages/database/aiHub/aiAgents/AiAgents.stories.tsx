import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import AiAgents from "./AiAgents";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/AI Hub/AI Agents/AI Agents",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface AiAgentsStoryArgs {
    hasAiAgent: boolean;
    isSharded: boolean;
    databaseAccess: databaseAccessLevel;
}

export const AiAgentsStory: StoryObj<AiAgentsStoryArgs> = {
    name: "AI Agents",
    render: (args) => {
        const { databases, accessManager, license } = mockStore;
        const { aiAgentService } = mockServices;

        const db = args.isSharded
            ? databases.withActiveDatabase_Sharded()
            : databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        license.with_License({
            HasAiAgent: args.hasAiAgent,
        });

        aiAgentService.withAiAgents();
        aiAgentService.withGenerateCode();

        return <AiAgents />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        hasAiAgent: true,
        isSharded: false,
        databaseAccess: "DatabaseAdmin",
    },
};
