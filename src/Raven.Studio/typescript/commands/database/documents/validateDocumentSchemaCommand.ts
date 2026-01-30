import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class validateDocumentSchemaCommand extends commandBase {
    constructor(private db: database | string, private document: documentDto) {
        super();
    }

    execute(): JQueryPromise<ValidateDocumentResult> {
        const url = endpoints.databases.studioDocument.studioValidateSchema;

        return this.post<ValidateDocumentResult>(url, JSON.stringify(this.document), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to validate document", response.responseText, response.statusText)
            )
    }
}

export = validateDocumentSchemaCommand;
