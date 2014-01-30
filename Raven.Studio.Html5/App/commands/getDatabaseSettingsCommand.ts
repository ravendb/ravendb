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

        var documentResult = $.Deferred();
        var result = this.query("/admin/databases/" + this.db.name, null);
        result.fail(response => documentResult.fail(response));
        result.done((queryResult: queryResultDto) => {
            documentResult.resolve(new document(queryResult));
        });
        return documentResult;
    }
}

export = getDatabaseSettingsCommand;