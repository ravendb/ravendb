import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");

class getDatabaseSettingsCommand extends commandBase {

    constructor(private db: database, private reportRefreshProgress = false) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        if (this.reportRefreshProgress) {
            this.reportInfo("Fetching Database Settings for '" + this.db.name + "'");
        }

        var resultsSelector = (queryResult: queryResultDto) => new document(queryResult);
        var url = "/admin/databases/" + this.db.name;//TODO: use endpoints
        var getTask = this.query(url, null, null, resultsSelector);

        if (this.reportRefreshProgress) {
            getTask.done(() => this.reportSuccess("Database Settings of '" + this.db.name + "' were successfully refreshed!"));
            getTask.fail((response: JQueryXHR) => this.reportError("Failed to refresh Database Settings!", response.responseText, response.statusText));
        }
        return getTask;
    }
}

export = getDatabaseSettingsCommand;
