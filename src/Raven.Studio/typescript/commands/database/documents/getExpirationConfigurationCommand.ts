import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getExpirationConfigurationCommand extends commandBase {

    private readonly db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration>();
        const url = endpoints.databases.expiration.expirationConfig;
        this.query(url, null, this.db)
            .done((expirationConfig: Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration) => deferred.resolve(expirationConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError("Failed to get expiration information", xhr.responseText, xhr.statusText);
                }
            });

        return deferred;
    }
}

export = getExpirationConfigurationCommand;
