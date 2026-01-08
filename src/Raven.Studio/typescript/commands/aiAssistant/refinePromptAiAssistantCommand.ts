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
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    RefinedPrompt: string;
}

export default class refinePromptAiAssistantCommand extends commandBase {
    constructor(private viewData: RefinePromptAiAssistantViewData, private abortSignal?: AbortSignal) {
        super();
    }

    execute() {
        const relativeUrl = endpoints.global.aiAssistant.assistantAssist + "?streaming=true";

        const dto: RefinePromptAiAssistantRequestDto = {
            ...this.viewData,
            OperationType: "RefinePrompt",
        };

        return this.fetch({
            relativeUrl,
            options: {
                method: "POST",
                body: JSON.stringify(dto),
                signal: this.abortSignal,
            },
        });
    }
}
