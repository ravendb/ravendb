import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");

class importDatabaseCommand extends commandBase {

    constructor(private db: database, private operationId: number, private file: File, private model: importDatabaseModel) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId
        };

        const url = endpoints.databases.smuggler.smugglerImport + this.urlEncodeArgs(urlArgs);
        const ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false
        };

        const formData = new FormData();
        const args = this.model.toDto();

        formData.append("importOptions", JSON.stringify(args, (key, value) => {
            if (key === "TransformScript" && value === "") {
                return undefined;
            }
            return value;
        }));

        formData.append("file", this.file);

        return this.post(url, formData, this.db, ajaxOptions, 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importDatabaseCommand; 
