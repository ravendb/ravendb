import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import EditAiAgent from "./EditAiAgent";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { AiAgentStubs } from "test/stubs/AiAgentStubs";
import { userEvent } from "storybook/test";

type AiAgentConfiguration = Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;

interface EditAiAgentStoryArgs {
    isExistingAgent: boolean;
    hasCustomAdvancedSettings: boolean;
    hasLongValues: boolean;
}

export default {
    title: "Pages/AI Hub/AI Agents/Edit AI Agent",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args) => {
        const { databases } = mockStore;
        const { aiAssistantService, aiAgentService, tasksService } = mockServices;

        databases.withActiveDatabase();

        tasksService.withConnectionStrings();
        aiAssistantService.withCheckConsent();
        aiAssistantService.withRefinePrompt();

        const agent = getAgent(args);
        aiAgentService.withAiAgents({ AiAgents: [agent] });

        return (
            <div style={{ height: "90vh" }}>
                <EditAiAgent queryParams={args.isExistingAgent ? { id: agent.Identifier } : undefined} />
            </div>
        );
    },
    args: {
        isExistingAgent: false,
        hasCustomAdvancedSettings: false,
        hasLongValues: false,
    },
} satisfies Meta<EditAiAgentStoryArgs>;

type Story = StoryObj<EditAiAgentStoryArgs>;

const openAdvancedTrimmingSettings: Story["play"] = async ({ canvas }) => {
    const advancedSettingsButtons = await canvas.findAllByRole("button", { name: "Advanced settings" });
    await userEvent.click(advancedSettingsButtons[advancedSettingsButtons.length - 1]);
};

export const Default: Story = {};

export const ExistingAgent: Story = {
    args: {
        isExistingAgent: true,
    },
    play: openAdvancedTrimmingSettings,
};

export const CustomAdvancedSettings: Story = {
    args: {
        isExistingAgent: true,
        hasCustomAdvancedSettings: true,
    },
    play: openAdvancedTrimmingSettings,
};

export const LongValues: Story = {
    args: {
        isExistingAgent: true,
        hasLongValues: true,
    },
    play: openAdvancedTrimmingSettings,
};

function getAgent(args: EditAiAgentStoryArgs): AiAgentConfiguration {
    const agent = AiAgentStubs.getAiAgents().AiAgents[0];

    if (args.hasLongValues) {
        return {
            ...agent,
            MaxModelIterationsPerCall: 2147483647,
            ChatTrimming: {
                History: null,
                Tokens: {
                    MaxTokensBeforeSummarization: 9007199254740991,
                    MaxTokensAfterSummarization: 9007199254740991,
                    SummarizationTaskBeginningPrompt: LONG_TEXT.repeat(40),
                    SummarizationTaskEndPrompt: LONG_TEXT.repeat(40),
                    ResultPrefix: LONG_TEXT.repeat(10),
                },
            },
        };
    }

    if (args.hasCustomAdvancedSettings) {
        return {
            ...agent,
            MaxModelIterationsPerCall: 30,
            ChatTrimming: {
                History: null,
                Tokens: {
                    MaxTokensBeforeSummarization: 64000,
                    MaxTokensAfterSummarization: 8000,
                    SummarizationTaskBeginningPrompt:
                        "Summarize this schema migration planning session.\nPreserve every table name and every decision.",
                    SummarizationTaskEndPrompt: "Summarize the conversation above under those rules.",
                    ResultPrefix: "Planning session so far: ",
                },
            },
        };
    }

    return {
        ...agent,
        ChatTrimming: {
            History: null,
            Tokens: {
                MaxTokensBeforeSummarization: null,
                MaxTokensAfterSummarization: null,
                SummarizationTaskBeginningPrompt:
                    "Summarize the following AI conversation into a concise, linear narrative that retains all critical information.",
                SummarizationTaskEndPrompt:
                    "Reminder:\r\n- Go over the entire previous conversation and summarize that according to the original instructions.",
                ResultPrefix: "Summary of previous conversation: ",
            },
        },
    };
}

const LONG_TEXT = "SummarizeTheConversationAndKeepEveryIdentifierThatWasMentioned_";
