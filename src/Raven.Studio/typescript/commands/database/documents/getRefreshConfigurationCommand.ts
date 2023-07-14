import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;

class getRefreshConfigurationCommand extends commandBase {

    private readonly db: database;
    
    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<RefreshConfiguration> {

        const deferred = $.Deferred<RefreshConfiguration>();
        const url = endpoints.databases.refresh.refreshConfig;
        this.query(url, null, this.db)
            .done((refreshConfig: RefreshConfiguration) => deferred.resolve(refreshConfig))
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
