import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import DocumentsCompressionConfiguration = Raven.Client.ServerWide.DocumentsCompressionConfiguration;

class saveDocumentsCompressionCommand extends commandBase {
    private readonly db: database | string;
    private readonly config: DocumentsCompressionConfiguration;

    constructor(db: database | string, config: DocumentsCompressionConfiguration) {
        super();
        this.db = db;
        this.config = config;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.documentsCompression.adminDocumentsCompressionConfig;
        
        return this.post<updateDatabaseConfigurationsResult>(url, JSON.stringify(this.config), this.db)
            .done(() => this.reportSuccess("Documents compression configuration was successfully saved"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save documents compression configuration", response.responseText, response.statusText));
    }
}

export = saveDocumentsCompressionCommand;
