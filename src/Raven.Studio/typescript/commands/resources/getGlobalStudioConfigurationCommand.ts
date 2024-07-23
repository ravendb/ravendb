import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import accessManager = require("common/shell/accessManager");

class getGlobalStudioConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationStudio;
        const loadTask = $.Deferred<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>(); 

        const resultsSelector = (dto: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration, xhr: JQueryXHR): Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration => {
            this.processHeaders(xhr);
            return dto;
        };

        this.query<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>(url, null, null, resultsSelector)
            .done((dto) => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                this.processHeaders(response);

                if (response.status !== 404) {
                    this.reportError(`Failed to load the studio global configuration`, response.responseText, response.statusText);
                    loadTask.reject(response);
                } else {
                    loadTask.resolve(null);
                }
            });
        
        return loadTask;
    }

    private processHeaders(response: JQueryXHR) {
        accessManager.default.allowEncryptedDatabasesOverHttp(response.getResponseHeader("AllowEncryptedDatabasesOverHttp") == "true");
    }
}

export = getGlobalStudioConfigurationCommand;
