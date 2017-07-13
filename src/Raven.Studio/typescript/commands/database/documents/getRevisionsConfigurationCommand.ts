import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getRevisionsConfigurationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Revisions.RevisionsConfiguration> {

        const deferred = $.Deferred<Raven.Client.Server.Revisions.RevisionsConfiguration>();
        const url = endpoints.databases.revisions.revisionsConfig;
        this.query(url, null, this.db)
            .done((revisionsConfig: Raven.Client.Server.Revisions.RevisionsConfiguration) => deferred.resolve(revisionsConfig))
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

export = getRevisionsConfigurationCommand;
