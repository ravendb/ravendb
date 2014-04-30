import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class saveDatabaseSettingsCommand extends commandBase {

    constructor(private db: database, private document: document) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Database Settings for '" + this.db.name + "'...");

        //var resultsSelector = (queryResult: queryResultDto) => new document(queryResult);


        var args = JSON.stringify(this.document.toDto());
        var url = "/admin/databases/" + this.db.name;
        var saveTask = this.post(url, args, null, { dataType: undefined });

        //var url = "/admin/databases/" + this.db.name;
        //var saveTask = this.query(url, null, null, resultsSelector);
        saveTask.done(() => this.reportSuccess("Database Settings of '" + this.db.name + "' were successfully saved!"));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save Database Settings!", response.responseText, response.statusText));
        return saveTask;
    }
}

export = saveDatabaseSettingsCommand;