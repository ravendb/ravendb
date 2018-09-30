import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");

class importDatabaseCommand extends commandBase {

    constructor(private db: database, private operationId: number, private file: File, private model: importDatabaseModel,
        private isUploading: KnockoutObservable<boolean>, private uploadStatus: KnockoutObservable<number>) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId
        };

        const url = endpoints.databases.smuggler.smugglerImport + this.urlEncodeArgs(urlArgs);

        const formData = new FormData();
        const args = this.model.toDto();

        formData.append("importOptions", JSON.stringify(args, (key, value) => {
            if (key === "TransformScript" && value === "") {
                return undefined;
            }
            return value;
        }));

        formData.append("file", this.file);

        return this.post(url, formData, this.db, commandBase.getOptionsForImport(this.isUploading, this.uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importDatabaseCommand; 
