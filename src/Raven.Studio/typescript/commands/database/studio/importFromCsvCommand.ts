import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class importFromCsvCommand extends commandBase {

    constructor(private db: database, private operationId: number, private file: File, private collectionName: string) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId,
            collection: this.collectionName || undefined
        };

        const url = endpoints.databases.smuggler.smugglerImportCsv + this.urlEncodeArgs(urlArgs);
        const ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false
        };

        const formData = new FormData();

        formData.append("file", this.file);

        return this.post(url, formData, this.db, ajaxOptions, 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importFromCsvCommand; 
