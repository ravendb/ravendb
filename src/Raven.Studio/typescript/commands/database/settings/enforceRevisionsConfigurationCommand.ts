import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class enforceRevisionsConfigurationCommand extends commandBase {
    private readonly db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.adminRevisions.adminRevisionsConfigEnforce;

        return this.post<void>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to enforce revisions configuration", response.responseText, response.statusText); 
            });
    }
}

export = enforceRevisionsConfigurationCommand; 

