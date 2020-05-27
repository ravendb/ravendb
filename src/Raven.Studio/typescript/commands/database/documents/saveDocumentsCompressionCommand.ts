import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveDocumentsCompressionCommand extends commandBase {
    constructor(private db: database, private documentsCompressionConfiguration: Raven.Client.ServerWide.DocumentsCompressionConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.documentsCompression.adminDocumentsCompressionConfig;
        const args = ko.toJSON(this.documentsCompressionConfiguration);
        
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .done(() => this.reportSuccess("Documents compression configuration was successfully saved"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save documents compression configuration", response.responseText, response.statusText));
    }
}

export = saveDocumentsCompressionCommand;
