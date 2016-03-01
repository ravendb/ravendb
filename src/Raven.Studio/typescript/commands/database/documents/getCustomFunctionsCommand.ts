import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import commandBase = require("commands/commandBase");

class getCustomFunctionsCommand extends commandBase {

    constructor(private db: database, private global = false) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<customFunctions> {
        var resultsSelector = (queryResult: customFunctionsDto) => new customFunctions(queryResult);
        var url = this.global ?  "/document?id=Raven/Global/Javascript/Functions" : "/document?id=Raven/Javascript/Functions";
        return this.query(url, null, this.db, resultsSelector);
    }
}

export = getCustomFunctionsCommand;
