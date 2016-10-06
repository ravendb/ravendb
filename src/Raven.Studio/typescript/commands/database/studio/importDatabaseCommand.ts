import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");

class importDatabaseCommand extends commandBase {

    constructor(private fileData: FormData, private model: importDatabaseModel, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        this.reportInfo("Importing data...");

        const args = this.model.toDto();
        const url = endpoints.databases.smuggler.smugglerImport + this.urlEncodeArgs(args);
        const ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false
        }
        return this.post(url, this.fileData, this.db, ajaxOptions)
            .done(() => this.reportInfo("Data was uploaded successfully, processing..."))
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importDatabaseCommand; 
