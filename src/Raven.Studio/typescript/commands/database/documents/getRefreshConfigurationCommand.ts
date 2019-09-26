import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getRefreshConfigurationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Refresh.RefreshConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.Refresh.RefreshConfiguration>();
        const url = endpoints.databases.refresh.refreshConfig;
        this.query(url, null, this.db)
            .done((refreshConfig: Raven.Client.Documents.Operations.Refresh.RefreshConfiguration) => deferred.resolve(refreshConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError("Failed to get refresh information", xhr.responseText, xhr.statusText);
                }
            });

        return deferred;
    }
}

export = getRefreshConfigurationCommand;
