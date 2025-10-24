import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type AiAssistantView = "GenAI" | "AI Agents";

interface RefinePromptAiAssistantRequestDto {
    OperationType: "RefinePrompt";
    View: AiAssistantView;
    Message: string;
};

export type RefinePromptAiAssistantViewData = Omit<RefinePromptAiAssistantRequestDto, "OperationType">;

export interface RefinePromptAiAssistantResultDto {
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    RefinedPrompt?: string;
}

export default class refinePromptAiAssistantCommand extends commandBase {
    constructor(private viewData: RefinePromptAiAssistantViewData) {
        super();
    }

    execute(): JQueryPromise<RefinePromptAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantAssist;

        const dto: RefinePromptAiAssistantRequestDto = {
            OperationType: "RefinePrompt",
            View: this.viewData.View,
            Message: this.viewData.Message,
        };

        return this.post<RefinePromptAiAssistantResultDto>(url, JSON.stringify(dto)).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI Assistant", response.responseText, response.statusText)
        );
    }
}
