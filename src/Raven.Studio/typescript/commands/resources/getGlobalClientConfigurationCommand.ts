import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getGlobalClientConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.ClientConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationClient;
        const loadTask = $.Deferred<Raven.Client.Documents.Operations.Configuration.ClientConfiguration>(); 
        
        this.query<Raven.Client.Documents.Operations.Configuration.ClientConfiguration>(url, null)
            .done(dto => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                if (response.status !== 404) {
                    this.reportError(`Failed to load client configuration`, response.responseText, response.statusText);
                    loadTask.reject(response);
                } else {
                    loadTask.resolve(null);
                }
            });
        
        return loadTask;
        
    }
    
}

export = getGlobalClientConfigurationCommand;
