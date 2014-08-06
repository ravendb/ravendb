import commandBase = require("commands/commandBase");
import database = require("models/database");
import customFunctions = require("models/customFunctions");

class saveCustomFunctionsCommand extends commandBase {
    constructor(private db: database, private toSave: customFunctions) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<any> {
        var args = JSON.stringify(this.toSave.toDto());
        var url = "/docs/Raven/Javascript/Functions";
        var saveTask = this.put(url, args, this.db, null);

        saveTask.done(() => this.reportSuccess("Custom functions saved!"));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save custom functions!", response.responseText, response.statusText));
        return saveTask;
    }
}

export = saveCustomFunctionsCommand;