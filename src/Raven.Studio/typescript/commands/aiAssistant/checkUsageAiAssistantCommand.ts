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

        const deferred = $.Deferred<CheckUsageAiAssistantResultDto>();

        this.query<CheckUsageAiAssistantResultDto>(url, null)
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
