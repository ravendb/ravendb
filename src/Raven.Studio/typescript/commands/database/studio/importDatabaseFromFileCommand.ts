/// <reference path="../../../../typings/tsd.d.ts" />

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class importDatabaseFromFileCommand extends commandBase {

    constructor(private db: database | string, private operationId: number, private file: File,
        private importOptions: Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions,
        private onUploadProgress: (percentComplete: number) => void) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId
        };

        const url = endpoints.databases.smuggler.smugglerImport + this.urlEncodeArgs(urlArgs);

        const formData = new FormData();

        formData.append("importOptions", JSON.stringify(this.importOptions, (key, value) => {
            if (key === "TransformScript" && value === "") {
                return undefined;
            }
            return value;
        }));

        formData.append("file", this.file);

        const isUploading = ko.observable<boolean>(false);
        const uploadStatus = ko.observable<number>(0);
        uploadStatus.subscribe((percent) => this.onUploadProgress(percent));

        return this.post(url, formData, this.db, commandBase.getOptionsForImport(isUploading, uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
    }
}

export = importDatabaseFromFileCommand;
