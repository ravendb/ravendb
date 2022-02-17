import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getGlobalStudioConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationStudio;
        const loadTask = $.Deferred<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>(); 
        
        this.query<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>(url, null)
            .done(dto => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                if (response.status !== 404) {
                    this.reportError(`Failed to load the studio global configuration`, response.responseText, response.statusText);
                    loadTask.reject(response);
                } else {
                    loadTask.resolve(null);
                }
            });
        
        return loadTask;
    }
}

export = getGlobalStudioConfigurationCommand;
