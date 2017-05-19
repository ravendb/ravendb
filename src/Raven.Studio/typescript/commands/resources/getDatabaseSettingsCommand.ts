import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getDatabaseSettingsCommand extends commandBase {

    constructor(private db: database, private reportRefreshProgress = false) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        const resultsSelector = (queryResult: queryResultDto<documentDto>) => new document(queryResult);
        const args = {
            name: this.db.name
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);

        const getTask = this.query(url, null, null, resultsSelector);

        if (this.reportRefreshProgress) {
            getTask.done(() => this.reportSuccess("Database Settings of '" + this.db.name + "' were successfully refreshed!"));
            getTask.fail((response: JQueryXHR) => this.reportError("Failed to refresh Database Settings!", response.responseText, response.statusText));
        }
        return getTask;
    }
}

export = getDatabaseSettingsCommand;
