import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import customFunctions = require("models/database/documents/customFunctions");
import endpoints = require("endpoints");

class saveCustomFunctionsCommand extends commandBase {

    static readonly documentId = "Raven/Javascript/Functions";

    constructor(private db: database, private toSave: customFunctions) {
        super();
    }

    private validateCustomFunctions(document: string): JQueryPromise<string> {
        return this.post(endpoints.databases.studioTasks.studioTasksValidateCustomFunctions, document, this.db, { dataType: 'text' });
    }

    execute(): JQueryPromise<void> {
        if (this.toSave.hasEmptyScript) {
            return this.deleteCustomFunctions();
        } else {
            const args = JSON.stringify(this.toSave.toDto());

            return this.validateCustomFunctions(args)
                .fail((response) => this.reportError("Failed to validate custom functions!", response.responseText, response.statusText))
                .then(() => {
                    return this.saveCustomFunctionsDocument(args);
                });
        }
    }

    private deleteCustomFunctions() {
        const args = {
            id: saveCustomFunctionsCommand.documentId
        };
        const url = endpoints.databases.document.docs + this.urlEncodeArgs(args);
        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess("Custom functions saved."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save custom functions.", response.responseText, response.statusText));
    }

    private saveCustomFunctionsDocument(args: string) {
        const urlArgs = { id: saveCustomFunctionsCommand.documentId };

        const url = endpoints.databases.document.docs + this.urlEncodeArgs(urlArgs);
        const saveTask = this.put<void>(url, args, this.db, null)
            .done(() => this.reportSuccess("Custom functions saved."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save custom functions.", response.responseText, response.statusText));
        return saveTask;
    }
}

export = saveCustomFunctionsCommand;