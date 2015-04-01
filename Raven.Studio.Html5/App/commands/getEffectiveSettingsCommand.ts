import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class getEffectiveSettingsCommand extends commandBase {Effective

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        var resultsSelector = (queryResult: queryResultDto) => new document(queryResult);
        var url = "/configuration/global/settings";
        return this.query(url, null, null, resultsSelector);
    }
}

export = getEffectiveSettingsCommand;