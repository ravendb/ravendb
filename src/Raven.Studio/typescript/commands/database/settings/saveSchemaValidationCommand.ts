import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;

class saveSchemaValidationCommand extends commandBase {
    constructor(private db: database | string, private schemaValidation: SchemaValidationConfiguration) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.schemaValidation.adminSchemaValidationConfig;

        return this.post<void>(url, JSON.stringify(this.schemaValidation), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to save schema validation configuration", response.responseText, response.statusText)
            )
            .done(() =>
                this.reportSuccess("Schema validation configuration saved successfully")
            );
    }
}

export = saveSchemaValidationCommand;
