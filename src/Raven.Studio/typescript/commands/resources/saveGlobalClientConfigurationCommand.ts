import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

class saveGlobalClientConfigurationCommand extends commandBase {
    private dto: ClientConfiguration;

    constructor(dto: ClientConfiguration) {
        super();
        this.dto = dto;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminConfiguration.adminConfigurationClient;
        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(`Failed to save client configuration`, response.responseText, response.statusText)) 
            .done(() => this.reportSuccess("Client Configuration was saved successfully"));
    }
}

export = saveGlobalClientConfigurationCommand;
