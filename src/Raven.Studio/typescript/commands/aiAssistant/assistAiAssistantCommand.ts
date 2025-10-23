import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type AiAssistantOperationType = "RefinePrompt" | "Chatbot";
type AiAssistantView = "GenAI" | "AI Agents";

export interface AssistAiAssistantRequestDto {
    OperationType: AiAssistantOperationType;
    View: AiAssistantView;
    Message: string;
};

export interface AssistAiAssistantResultDto {
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    RefinedPrompt?: string;
}

export default class assistAiAssistantCommand extends commandBase {
    constructor(private dto: AssistAiAssistantRequestDto) {
        super();
    }

    execute(): JQueryPromise<AssistAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantAssist;

        return this.post<AssistAiAssistantResultDto>(url, JSON.stringify(this.dto)).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI Assistant", response.responseText, response.statusText)
        );
    }
}
