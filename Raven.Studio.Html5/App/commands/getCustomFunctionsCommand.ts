import database = require("models/database");
import customFunctions = require("models/customFunctions");
import commandBase = require("commands/commandBase");

class getCustomFunctionsCommand extends commandBase {

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<customFunctions> {
        var resultsSelector = (queryResult: customFunctionsDto) => new customFunctions(queryResult);
        var url = "/docs/Raven/Studio/Config";
        return this.query(url, null, null, resultsSelector);
    }
}

export = getCustomFunctionsCommand;