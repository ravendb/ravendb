import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface CheckUsageAiAssistantResultDto {
    Status: Extract<AiAssistantResponseStatus, "Success" | "InvalidCredentials">;
    UsagePercentage?: number;
}

export default class checkUsageAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<CheckUsageAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantCheckUsage;

        return this.query<CheckUsageAiAssistantResultDto>(url, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to check usage for AI Assistant", response.responseText, response.statusText)
        );
    }
}
