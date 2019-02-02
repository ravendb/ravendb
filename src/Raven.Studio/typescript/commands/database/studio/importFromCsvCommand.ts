import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class importFromCsvCommand extends commandBase {

    constructor(private db: database, private operationId: number, private file: File, private collectionName: string,
        private isUploading: KnockoutObservable<boolean>, private uploadStatus: KnockoutObservable<number>) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId,
            collection: this.collectionName || undefined
        };

        const url = endpoints.databases.smuggler.smugglerImportCsv + this.urlEncodeArgs(urlArgs);

        const formData = new FormData();
        formData.append("file", this.file);

        return this.post(url, formData, this.db, commandBase.getOptionsForImport(this.isUploading, this.uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importFromCsvCommand; 
