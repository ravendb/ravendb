import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveGlobalClientConfigurationCommand extends commandBase {
    
    constructor(private dto: Raven.Client.Documents.Operations.Configuration.ClientConfiguration) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminConfiguration.adminConfigurationClient;
        
        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(`Failed to save client configuration`, response.responseText, response.statusText)) 
            .done(() => this.reportSuccess("Client Configuration was saved successfully"));
    }
}

export = saveGlobalClientConfigurationCommand;
