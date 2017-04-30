import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import endpoints = require("endpoints");

class saveCustomFunctionsCommand extends commandBase {
    constructor(private db: database, private toSave: customFunctions) {
        super();
    }

    private validateCustomFunctions(document: string): JQueryPromise<any> {
        return this.post(endpoints.databases.studioTasks.studioTasksValidateCustomFunctions, document, this.db, { dataType: 'text' }); //TODO use right validation
    }

    execute(): JQueryPromise<Raven.Client.Documents.Commands.Batches.PutResult> {
        const args = JSON.stringify(this.toSave.toDto());

        return this.validateCustomFunctions(args)
            .fail((response) => this.reportError("Failed to validate custom functions!", response.responseText, response.statusText))
            .then(() => {
                const urlArgs = { id: "Raven/Javascript/Functions" };

                const url = endpoints.databases.document.docs + this.urlEncodeArgs(urlArgs);
                const saveTask = this.put(url, args, this.db, null)
                    .done(() => this.reportSuccess("Custom functions saved!"))
                    .fail((response: JQueryXHR) => this.reportError("Failed to save custom functions!", response.responseText, response.statusText));
                return saveTask;
            });
    }
}

export = saveCustomFunctionsCommand;