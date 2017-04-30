import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCustomFunctionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<customFunctions> {
        const urlArgs = { id: "Raven/Javascript/Functions" };

        const resultsSelector = (queryResult: customFunctionsDto) => new customFunctions(queryResult);
        const url = endpoints.databases.document.docs;
        return this.query(url, urlArgs, this.db, resultsSelector);
    }
}

export = getCustomFunctionsCommand;