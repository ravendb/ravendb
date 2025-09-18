import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export type AssistAiAssistantRequestDto =
    | ({
          OperationType: Extract<Raven.Server.Documents.AI.AiAssistant.AiAssistantOperationType, "RefineText">;
      } & Omit<
          Raven.Server.Documents.AI.AiAssistant.Requests.RefineTextRequest,
          "OperationType" | "CertificateThumbprint" | "License"
      >)
    | ({
          OperationType: Extract<Raven.Server.Documents.AI.AiAssistant.AiAssistantOperationType, "RefineGenAiPrompt">;
      } & Omit<
          Raven.Server.Documents.AI.AiAssistant.Requests.RefineGenAiPromptRequest,
          "OperationType" | "CertificateThumbprint" | "License"
      >);

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
            this.reportError("Failed to run AI assistant", response.responseText, response.statusText)
        );
    }
}
