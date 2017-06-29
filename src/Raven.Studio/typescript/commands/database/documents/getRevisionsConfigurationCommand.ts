import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getVersioningCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Versioning.VersioningConfiguration> {

        const deferred = $.Deferred<Raven.Client.Server.Versioning.VersioningConfiguration>();
        const url = endpoints.databases.versioning.versioningConfig;
        this.query(url, null, this.db)
            .done((versioningConfig: Raven.Client.Server.Versioning.VersioningConfiguration) => deferred.resolve(versioningConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                }
                
            });

        return deferred;
    }
}

export = getVersioningCommand;
