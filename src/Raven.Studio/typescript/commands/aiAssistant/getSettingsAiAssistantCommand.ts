import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface GetSettingsAiAssistantResultDto {
    DisableAiAssistant: boolean;
    DisableDataSubmission: boolean;
}

export default class getSettingsAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<GetSettingsAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantSettings;

        return this.query<GetSettingsAiAssistantResultDto>(url, null);
    }
}
