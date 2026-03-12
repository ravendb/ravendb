import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./ChatAiAgent.stories";
import { composeStories } from "@storybook/react-webpack5";
import document from "models/database/documents/document";
import { AiAgentDocMessage } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";

const { ChatAiAgentStory } = composeStories(Stories);

const selectors = {
    promptPlaceholder: "Ask the agent anything",
} as const;

function createDocumentWithMessages(messages: AiAgentDocMessage[]): document {
    return new document({
        Messages: messages,
    });
}

describe("AiAgentMessages", () => {
    it("can show prompt for regular conversation", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ChatAiAgentStory isHistory={false} />);

        expect(screen.getByPlaceholderText(selectors.promptPlaceholder)).toBeInTheDocument();
    });

    it("can hide prompt for history conversation", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ChatAiAgentStory isHistory />);

        expect(screen.queryByPlaceholderText(selectors.promptPlaceholder)).not.toBeInTheDocument();
    });

    it("can show system prompt", async () => {
        const systemPromptText = "This is a system prompt";

        const conversationDocument = createDocumentWithMessages([
            {
                role: "system",
                content: systemPromptText,
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.getByText("System prompt")).toBeInTheDocument();
        expect(screen.getByText(systemPromptText)).toBeInTheDocument();
    });

    it("can hide user parameters message", async () => {
        const conversationDocument = createDocumentWithMessages([
            {
                role: "user",
                content: "AI Agent Parameters:\ncompany = companies/90-A\r\n",
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.queryByText("AI Agent Parameters")).not.toBeInTheDocument();
    });

    it("can show query tool", async () => {
        const conversationDocument = createDocumentWithMessages([
            {
                role: "assistant",
                content: null,
                tool_calls: [
                    {
                        id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                        type: "function",
                        function: {
                            name: "QueryRecentCategories",
                            arguments: "{}",
                        },
                    },
                ],
            },
            {
                tool_call_id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                role: "tool",
                content: '[{"Name":"Beverages","Description":"Soft drinks, coffees, teas, beers, and ales"}]',
            },
            {
                role: "assistant",
                content:
                    '{"Answer":"I ran QueryRecentCategories for company companies/90-A and found 1 recent categories:\\n1) Beverages (categories/1-A) — Soft drinks, coffees, teas, beers, and ales"}',
            },
        ]);

        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        const transcriptButton = screen.getByText("Query tool: QueryRecentCategories");
        await fireClick(transcriptButton);
        expect(screen.getByText("Parameters filled by LLM")).toBeInTheDocument();
        expect(screen.getByText("Query tool result")).toBeInTheDocument();

        const seeDetailsButton = screen.getByText("See details");
        await fireClick(seeDetailsButton);
        expect(screen.getByText("Query")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Test query" })).toBeInTheDocument();
    });

    it("can show action tool to submit", async () => {
        const conversationDocument = createDocumentWithMessages([
            {
                content: null,
                role: "assistant",
                tool_calls: [
                    {
                        function: {
                            arguments: '{"Query":["test"]}',
                            name: "ActionProductSearch",
                        },
                        id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                        type: "function",
                    },
                ],
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.getByText("Action tool: ActionProductSearch")).toBeInTheDocument();

        expect(screen.getByText(/Enter a response after completing action/)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Submit" })).toBeInTheDocument();
    });

    it("can show submitted action tool", async () => {
        const conversationDocument = createDocumentWithMessages([
            {
                content: null,
                role: "assistant",
                tool_calls: [
                    {
                        function: {
                            arguments: '{"Query":["test"]}',
                            name: "ActionProductSearch",
                        },
                        id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                        type: "function",
                    },
                ],
            },
            {
                tool_call_id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                role: "tool",
                content: "Submitted content",
            },
            {
                role: "assistant",
                content: "LLM answer",
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.getByText("Action tool: ActionProductSearch")).toBeInTheDocument();

        expect(screen.getByText(/Response from action tool/)).toBeInTheDocument();
        expect(screen.getByText("Submitted")).toBeInTheDocument();
    });
});
