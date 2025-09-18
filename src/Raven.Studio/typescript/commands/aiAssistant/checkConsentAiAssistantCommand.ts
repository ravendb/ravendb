import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface CheckConsentAiAssistantResultDto {
    Status: AiAssistantResponseStatus;
}

export default class checkConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<CheckConsentAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantCheckConsent;

        return this.query<CheckConsentAiAssistantResultDto>(url, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to check consent for AI assistant", response.responseText, response.statusText)
        );
    }
}
