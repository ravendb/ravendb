import { AssistAiAssistantResultDto } from "commands/aiAssistant/assistAiAssistantCommand";

export class AiAssistantStubs {
    static assist(): AssistAiAssistantResultDto {
        return {
            InputTokenCount: 10,
            OutputTokenCount: 20,
            UsagePercentage: 1,
            RefinedPrompt: "This is your refined prompt",
            RefinedText: "This is your refined text",
            Status: "Success",
        };
    }
}
