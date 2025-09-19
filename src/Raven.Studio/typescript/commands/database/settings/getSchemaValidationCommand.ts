import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
class getSchemaValidationCommand extends commandBase {
    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration> {
        const url = endpoints.databases.schemaValidation.schemaValidationConfig;

        return this.query<Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get schema validations", response.responseText, response.statusText));
    }
}

export = getSchemaValidationCommand;
