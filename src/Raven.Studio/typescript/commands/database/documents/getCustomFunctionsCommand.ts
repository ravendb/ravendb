import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCustomFunctionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<customFunctions> {
        const task = $.Deferred<customFunctions>();

        const url = endpoints.databases.studioCustomFunctions.studioCustomFunctions;
        this.query(url, null, this.db)
            .done((dto: Raven.Server.Documents.CustomFunctions) => task.resolve(new customFunctions(dto)))
            .fail((xhr: JQueryXHR) => task.reject(xhr));

        return task;
    }
}

export = getCustomFunctionsCommand;