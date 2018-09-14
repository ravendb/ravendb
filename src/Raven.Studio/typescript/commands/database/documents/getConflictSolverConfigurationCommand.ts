import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConflictSolverConfigurationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.ConflictSolver> {

        const deferred = $.Deferred<Raven.Client.ServerWide.ConflictSolver>();
        const url = endpoints.databases.replication.replicationConflictsSolver;
        this.query(url, null, this.db)
            .done((expirationConfig: Raven.Client.ServerWide.ConflictSolver) => deferred.resolve(expirationConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError("Failed to get conflict solver configuration", xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getConflictSolverConfigurationCommand;
