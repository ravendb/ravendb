import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface RunChatbotAssistAiAssistantRequestDto {
    OperationType: "Chatbot";
    View: string;
    Message: string;
    RavenVersion: string;
    ConversationId?: string;
};

export type RunChatbotAiAssistantViewData = Omit<RunChatbotAssistAiAssistantRequestDto, "OperationType">;

interface RelevantLink {
    Title: string;
    Url: string;
}

export interface RunChatbotAiAssistantResultDto {
    ConversationId: string;
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    Response: {
        Answer: string;
        RelevantLinks: RelevantLink[];
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
            ...this.viewData
        };

        return this.post<RunChatbotAiAssistantResultDto>(url, JSON.stringify(dto)).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI Assistant", response.responseText, response.statusText)
        );
    }
}
