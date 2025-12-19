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

        const deferred = $.Deferred<CheckConsentAiAssistantResultDto>();

        this.query<CheckConsentAiAssistantResultDto>(url, null)
            .done((result) => deferred.resolve(result))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 401 && xhr.responseJSON?.Status) {
                    deferred.resolve({
                        Status: xhr.responseJSON.Status,
                    });
                } else {
                    deferred.reject(xhr);
                }
            });

        return deferred;
    }
}
