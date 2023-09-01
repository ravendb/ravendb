import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDataArchivalConfigurationCommand extends commandBase {

    private readonly db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration>();
        const url = endpoints.databases.dataArchival.dataArchivalConfig;
        this.query(url, null, this.db)
            .done((expirationConfig: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration) => deferred.resolve(expirationConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError("Failed to get data archival configuration", xhr.responseText, xhr.statusText);
                }
            });

        return deferred;
    }
}

export = getDataArchivalConfigurationCommand;
