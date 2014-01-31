import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class getDatabaseSettingsCommand extends commandBase {

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        var resultsSelector = (queryResult: queryResultDto) => new document(queryResult);
        return this.query("/admin/databases/" + this.db.name, null, null, resultsSelector);
    }
}

export = getDatabaseSettingsCommand;