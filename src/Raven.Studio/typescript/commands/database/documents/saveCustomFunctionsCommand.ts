import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import endpoints = require("endpoints");

class saveCustomFunctionsCommand extends commandBase {

    static readonly documentId = "Raven/Javascript/Functions";

    constructor(private db: database, private toSave: customFunctions) {
        super();
    }

    execute(): JQueryPromise<string> {
        const args = JSON.stringify(this.toSave.toDto());

        let validationTask: JQueryPromise<string>;
        // 1. Validate scripts if not empty
        if (!this.toSave.hasEmptyScript) {
            validationTask = this.validateCustomFunctions(args)
                .fail((response) => {
                     return this.reportError("Failed to validate custom functions!", response.responseText, response.statusText);
                });
        } else {
            validationTask = $.Deferred<string>().resolve();
        }

        // 2. Send to server
        return validationTask.then(() => this.saveCustomFunctionsDocument(args));
    }

    private validateCustomFunctions(document: string): JQueryPromise<string> {
        return this.post(endpoints.databases.studioTasks.studioTasksValidateCustomFunctions, document, this.db, { dataType: 'text' });
    }

    private saveCustomFunctionsDocument(args: string) {
        const url = endpoints.global.adminDatabases.adminModifyCustomFunctions + this.urlEncodeArgs({ name: this.db.name });

        const saveTask = this.post<void>(url, args)
            .done(() => this.reportSuccess("Custom functions saved."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save custom functions.", response.responseText, response.statusText));

        return saveTask;
    }
}

export = saveCustomFunctionsCommand;