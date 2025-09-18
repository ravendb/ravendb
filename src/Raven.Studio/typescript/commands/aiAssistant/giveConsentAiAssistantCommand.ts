import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface GiveConsentAiAssistantResultDto {
    Status: AiAssistantResponseStatus;
}

export default class giveConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<GiveConsentAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantGiveConsent;

        return this.post(url, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to give consent to AI Assistant", response.responseText, response.statusText)
        );
    }
}

