import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class validateSchemaCommand extends commandBase {
    constructor(private db: database | string, private dto: ValidateSchemaRequestDto) {
        super();
    }

    execute(): JQueryPromise<ValidateSchemaResponseDto> {
        const url = endpoints.databases.schemaValidation.schemaValidationValidate;

        return this.post<ValidateSchemaResponseDto>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to validate schema configuration", response.responseText, response.statusText)
            )
    }
}

export = validateSchemaCommand;
