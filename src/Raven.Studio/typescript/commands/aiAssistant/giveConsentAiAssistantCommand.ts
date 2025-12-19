import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface GiveConsentAiAssistantResultDto {
    Status: Extract<AiAssistantResponseStatus, "Success" | "InvalidCredentials">;
}

export default class giveConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<GiveConsentAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantGiveConsent;

        const deferred = $.Deferred<GiveConsentAiAssistantResultDto>();

        this.post(url, null)
            .done((result) => deferred.resolve(result))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 401 && xhr.responseJSON?.Status) {
                    deferred.resolve({
                        Status: xhr.responseJSON.Status,
                    });
                } else {
                    this.reportError("Failed to give consent to AI Assistant", xhr.responseText, xhr.statusText);
                    deferred.reject(xhr);
                }
            });

        return deferred;
    }
}
