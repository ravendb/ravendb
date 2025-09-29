import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type AiAssistantOperationType = Raven.Server.Documents.AI.AiAssistant.AiAssistantOperationType;
type RefineTextRequest = Raven.Server.Documents.AI.AiAssistant.Requests.RefineTextRequest;
type RefineGenAiPromptRequest = Raven.Server.Documents.AI.AiAssistant.Requests.RefineGenAiPromptRequest;

type AiRequest<T extends AiAssistantOperationType, R> = {
    OperationType: Extract<AiAssistantOperationType, T>;
} & Omit<R, "OperationType" | "CertificateThumbprint" | "License">;

export type AssistAiAssistantRequestDto =
    | AiRequest<"RefineText", RefineTextRequest>
    | AiRequest<"RefineGenAiPrompt", RefineGenAiPromptRequest>;

export interface AssistAiAssistantResultDto {
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    RefinedText?: string;
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
