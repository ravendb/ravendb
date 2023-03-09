import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveAdminLogsMicrosoftConfigurationCommand extends commandBase {
    
    private readonly configuration: string;
    
    constructor(configuration: string) {
        super();
        this.configuration = configuration;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftConfiguration;

        return this.post<void>(url, this.configuration, null, { dataType: undefined })
            .done(() => this.reportSuccess("Microsoft logs configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set Microsoft logs configuration", response.responseText, response.statusText));
    }
}

export = saveAdminLogsMicrosoftConfigurationCommand;
