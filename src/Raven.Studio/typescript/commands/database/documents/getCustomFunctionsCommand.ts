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

        const task = $.Deferred<customFunctions>();

        const url = endpoints.databases.document.docs;
        this.query(url, urlArgs, this.db)
            .done((queryResult: queryResultDto<customFunctionsDto>) => task.resolve(new customFunctions(queryResult.Results[0])))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    task.resolve(null);
                } else {
                    task.reject(xhr);
                }
            });

        return task;
    }
}

export = getCustomFunctionsCommand;