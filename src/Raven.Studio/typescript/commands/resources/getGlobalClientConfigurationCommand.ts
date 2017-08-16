import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getGlobalClientConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.ServerWide.ClientConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationClient;
        return this.query<Raven.Client.ServerWide.ClientConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to load client configuration`, response.responseText, response.statusText)) 
    }
    
}

export = getGlobalClientConfigurationCommand;
