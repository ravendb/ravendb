import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./ChatAiAgent.stories";
import { composeStories } from "@storybook/react-webpack5";
import document from "models/database/documents/document";
import {
    AiAgentDocMessage,
    AiAgentOpenActionCalls,
} from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";

const { ChatAiAgentStory } = composeStories(Stories);

const selectors = {
    promptPlaceholder: "Ask the agent anything",
} as const;

function createDocumentWithMessages(
    messages: AiAgentDocMessage[],
    openActionCalls: AiAgentOpenActionCalls = {}
): document {
    return new document({
        Messages: messages,
        OpenActionCalls: openActionCalls,
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
                date: "2025-08-08T10:28:01.5884757Z",
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
                date: "2025-08-08T10:28:01.5884757Z",
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.queryByText("AI Agent Parameters")).not.toBeInTheDocument();
    });

    it("can show query tool", async () => {
        const queryToolName = "QueryRecentCategories";

        const conversationDocument = createDocumentWithMessages([
            {
                role: "assistant",
                content: null,
                tool_calls: [
                    {
                        id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                        type: "function",
                        function: {
                            name: queryToolName,
                            arguments: "{}",
                        },
                    },
                ],
                date: "2025-08-08T10:28:01.5884757Z",
            },
            {
                tool_call_id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                role: "tool",
                content: '[{"Name":"Beverages","Description":"Soft drinks, coffees, teas, beers, and ales"}]',
                date: "2025-08-08T10:28:02.5884757Z",
            },
        ]);

        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        const transcriptButton = screen.getByText("Query tool: " + queryToolName);
        await fireClick(transcriptButton);
        expect(screen.getByText("Parameters filled by LLM")).toBeInTheDocument();
        expect(screen.getByText("Query tool result")).toBeInTheDocument();

        const seeDetailsButton = screen.getByText("See details");
        await fireClick(seeDetailsButton);
        expect(screen.getByText("Query")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Test query" })).toBeInTheDocument();
    });

    it("can show action tool to submit", async () => {
        const actionToolName = "ActionProductSearch";

        const conversationDocument = createDocumentWithMessages(
            [
                {
                    content: null,
                    role: "assistant",
                    tool_calls: [
                        {
                            function: {
                                arguments: '{"Query":["test"]}',
                                name: actionToolName,
                            },
                            id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                            type: "function",
                        },
                    ],
                    date: "2025-08-08T10:28:01.5884757Z",
                },
            ],
            {
                call_MdKvWaFtl0cJAc5a0q26Lo97: {
                    Name: actionToolName,
                    ToolId: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                    Arguments: '{"Query":["test"]}',
                },
            }
        );

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.getByText("Action tool: " + actionToolName)).toBeInTheDocument();

        expect(screen.getByText(/Enter a response after completing action/)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Submit" })).toBeInTheDocument();
    });

    it("can show submitted action tool", async () => {
        const actionToolName = "ActionProductSearch";

        const conversationDocument = createDocumentWithMessages([
            {
                content: null,
                role: "assistant",
                tool_calls: [
                    {
                        function: {
                            arguments: '{"Query":["test"]}',
                            name: actionToolName,
                        },
                        id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                        type: "function",
                    },
                ],
                date: "2025-08-08T10:28:01.5884757Z",
            },
            {
                tool_call_id: "call_MdKvWaFtl0cJAc5a0q26Lo97",
                role: "tool",
                content: "Submitted content",
                date: "2025-08-08T10:28:02.5884757Z",
            },
        ]);

        const { screen } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        expect(screen.getByText("Action tool: " + actionToolName)).toBeInTheDocument();

        expect(screen.getByText(/Response from action tool/)).toBeInTheDocument();
        expect(screen.getByText("Submitted")).toBeInTheDocument();
    });

    it("can show sub-agent", async () => {
        const subAgentId = "raven-expert-agent";

        const conversationDocument = createDocumentWithMessages([
            {
                content: null,
                role: "assistant",
                tool_calls: [
                    {
                        function: {
                            arguments: '{"subAgentUserPrompt":"Explain how to query documents in RavenDB"}',
                            name: subAgentId,
                        },
                        id: "call_CscbrKZ4VC1GibM2oERi7nF9",
                        type: "function",
                    },
                ],
                date: "2025-08-08T10:28:01.5884757Z",
            },
            {
                tool_call_id: "call_CscbrKZ4VC1GibM2oERi7nF9",
                role: "tool",
                content: "Sub agent answer",
                subConversationId: "Chats/2",
                date: "2025-08-08T10:28:02.5884757Z",
            },
        ]);

        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        const transcriptButton = screen.getByText("Sub-agent: " + subAgentId);
        await fireClick(transcriptButton);
        expect(screen.getByText("Parameters filled by LLM")).toBeInTheDocument();
        expect(screen.getByText("Sub-conversation created")).toBeInTheDocument();
        expect(screen.getByText("Sub-agent final answer")).toBeInTheDocument();
    });

    it("can show unknown tool", async () => {
        const toolName = "unknown-tool";

        const conversationDocument = createDocumentWithMessages([
            {
                role: "assistant",
                content: null,
                tool_calls: [
                    {
                        id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                        type: "function",
                        function: {
                            name: toolName,
                            arguments: "{}",
                        },
                    },
                ],
                date: "2025-08-08T10:28:01.5884757Z",
            },
            {
                tool_call_id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                role: "tool",
                content: '[{"Name":"Beverages","Description":"Soft drinks, coffees, teas, beers, and ales"}]',
                date: "2025-08-08T10:28:02.5884757Z",
            },
        ]);

        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <ChatAiAgentStory conversationDocument={conversationDocument} />
        );

        const transcriptButton = screen.getByText("Tool: " + toolName);
        await fireClick(transcriptButton);
        expect(screen.getByText("Parameters filled by LLM")).toBeInTheDocument();
    });
});
