import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./AiAgents.stories";
import { composeStories } from "@storybook/react";

const { AiAgentsStory } = composeStories(Stories);

const selectors = {
    addButtonText: "Add new agent",
};

describe("AiAgents", () => {
    it("can render", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<AiAgentsStory />);
        expect(screen.getByRole("heading", { name: "AI Agents" })).toBeInTheDocument();
        expect(screen.getByText("First agent")).toBeInTheDocument();
    });

    it("can enable add button when HasAiAgents is true", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<AiAgentsStory hasAiAgent />);
        expect(screen.getByText(selectors.addButtonText)).not.toHaveClass("disabled");
    });

    it("can disable add button when HasAiAgents is false", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<AiAgentsStory hasAiAgent={false} />);
        expect(screen.getByText(selectors.addButtonText)).toHaveClass("disabled");
    });

    it("can hide add button when user does not have DatabaseAdmin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<AiAgentsStory databaseAccess="DatabaseReadWrite" />);
        expect(screen.queryByText(selectors.addButtonText)).not.toBeInTheDocument();
    });

    it("can not get agents when user does not have DatabaseAdmin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<AiAgentsStory databaseAccess="DatabaseReadWrite" />);
        expect(screen.getByText("No agents found")).toBeInTheDocument();
    });
});
