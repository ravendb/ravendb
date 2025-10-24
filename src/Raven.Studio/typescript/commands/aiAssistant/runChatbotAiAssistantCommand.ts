import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface RunChatbotAssistAiAssistantRequestDto {
    OperationType: "Chatbot";
    View: string;
    Message: string;
    ConversationId?: string;
};

export type RunChatbotAiAssistantViewData = Omit<RunChatbotAssistAiAssistantRequestDto, "OperationType">;

export interface RunChatbotAiAssistantResultDto {
    ConversationId: string;
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    Response: {
        Answer: string;
        RelevantLinks: string[]
    };
}

export default class runChatbotAiAssistantCommand extends commandBase {
    constructor(private viewData: RunChatbotAiAssistantViewData) {
        super();
    }

    execute(): JQueryPromise<RunChatbotAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantAssist;

        const dto: RunChatbotAssistAiAssistantRequestDto = {
            OperationType: "Chatbot",
            View: this.viewData.View,
            Message: this.viewData.Message,
            ConversationId: this.viewData.ConversationId,
        };

        return this.post<RunChatbotAiAssistantResultDto>(url, JSON.stringify(dto)).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI Assistant", response.responseText, response.statusText)
        );
    }
}
