import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveConflictSolverConfigurationCommand extends commandBase {
    private readonly db: database;
    private readonly configuration: Raven.Client.ServerWide.ConflictSolver;

    constructor(db: database, configuration: Raven.Client.ServerWide.ConflictSolver) {
        super();
        this.db = db;
        this.configuration = configuration;
    }

    execute(): JQueryPromise<updateConflictSolverConfigurationResponse> {
        const urlArgs = {
            name: this.db.name
        };
        const url = endpoints.global.adminDatabases.adminReplicationConflictsSolver + this.urlEncodeArgs(urlArgs);
        const args = ko.toJSON(this.configuration);
        return this.post<updateConflictSolverConfigurationResponse>(url, args)
            .done(() => this.reportSuccess("Conflict solver configuration was saved"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save conflict solver configuration", response.responseText, response.statusText));

    }
}

export = saveConflictSolverConfigurationCommand;
