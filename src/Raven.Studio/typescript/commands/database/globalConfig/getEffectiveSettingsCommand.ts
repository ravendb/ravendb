import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");

class getEffectiveSettingsCommand extends commandBase {

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        var resultsSelector = (queryResult: queryResultDto) => new document(queryResult);
        var url = "/configuration/global/settings";//TODO: use endpoints
        return this.query(url, null, null, resultsSelector);
    }
}

export = getEffectiveSettingsCommand;
