import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveAdminLogsMicrosoftConfigurationCommand extends commandBase {
    
    private readonly configuration: object;
    private readonly persist: boolean;
    
    constructor(configuration: object, persist: boolean) {
        super();
        this.configuration = configuration;
        this.persist = persist;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftConfiguration;

        const payload = {
            Configuration: this.configuration,
            Persist: this.persist
        };
        
        return this.post<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("Microsoft logs configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set Microsoft logs configuration", response.responseText, response.statusText));
    }
}

export = saveAdminLogsMicrosoftConfigurationCommand;
