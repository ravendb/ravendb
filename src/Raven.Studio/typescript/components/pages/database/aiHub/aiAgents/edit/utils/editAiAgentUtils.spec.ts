import { AiAgentStubs } from "test/stubs/AiAgentStubs";
import { editAiAgentUtils } from "./editAiAgentUtils";

type AiAgentConfiguration = Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;

function getAgentWithCustomAdvancedSettings(): AiAgentConfiguration {
    return {
        ...AiAgentStubs.getAiAgents().AiAgents[0],
        MaxModelIterationsPerCall: 30,
        ChatTrimming: {
            History: null,
            Tokens: {
                MaxTokensBeforeSummarization: 64000,
                MaxTokensAfterSummarization: 8000,
                SummarizationTaskBeginningPrompt: "Custom beginning prompt",
                SummarizationTaskEndPrompt: "Custom end prompt",
                ResultPrefix: "Custom prefix:",
            },
        },
    };
}

describe("editAiAgentUtils", () => {
    it("keeps the iterations limit and the summarization prompts when an agent is saved", () => {
        const agent = getAgentWithCustomAdvancedSettings();

        const savedAgent = editAiAgentUtils.mapToDto(editAiAgentUtils.mapFromDto(agent));

        expect(savedAgent.MaxModelIterationsPerCall).toBe(30);
        expect(savedAgent.ChatTrimming.Tokens).toEqual(agent.ChatTrimming.Tokens);
    });

    it("copies the iterations limit and the summarization prompts to a cloned agent", () => {
        const agent = getAgentWithCustomAdvancedSettings();

        const clonedAgent = editAiAgentUtils.mapToDto(editAiAgentUtils.mapFromDto(agent, true));

        expect(clonedAgent.MaxModelIterationsPerCall).toBe(30);
        expect(clonedAgent.ChatTrimming.Tokens).toEqual(agent.ChatTrimming.Tokens);
    });

    it("leaves them empty for a new agent, so the server sets its defaults", () => {
        const newAgent = editAiAgentUtils.mapToDto(editAiAgentUtils.mapFromDto());

        expect(newAgent.MaxModelIterationsPerCall).toBeNull();
        expect(newAgent.ChatTrimming.Tokens).toMatchObject({
            SummarizationTaskBeginningPrompt: null,
            SummarizationTaskEndPrompt: null,
            ResultPrefix: null,
        });
    });

    it("sends cleared summarization prompts as empty, so the server sets its defaults", () => {
        const formData = editAiAgentUtils.mapFromDto(getAgentWithCustomAdvancedSettings());

        const savedAgent = editAiAgentUtils.mapToDto({
            ...formData,
            trimming: {
                ...formData.trimming,
                summarizationTaskBeginningPrompt: "",
                summarizationTaskEndPrompt: "",
                summarizationResultPrefix: "",
            },
        });

        expect(savedAgent.ChatTrimming.Tokens).toMatchObject({
            SummarizationTaskBeginningPrompt: null,
            SummarizationTaskEndPrompt: null,
            ResultPrefix: null,
        });
    });
});
