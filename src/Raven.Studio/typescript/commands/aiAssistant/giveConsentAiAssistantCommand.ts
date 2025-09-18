import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class giveConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<any> {
        const url = endpoints.global.aiAssistant.assistantGiveConsent;

        return this.post(url, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to give consent to AI assistant", response.responseText, response.statusText)
        );
    }
}

export = giveConsentAiAssistantCommand;
