import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface CheckConsentAiAssistantResultDto {
    Status: Extract<AiAssistantResponseStatus, "Success" | "InvalidCredentials" | "ConsentRequired">;
}

export default class checkConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<CheckConsentAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantCheckConsent;

        return this.query<CheckConsentAiAssistantResultDto>(url, null);
    }
}
