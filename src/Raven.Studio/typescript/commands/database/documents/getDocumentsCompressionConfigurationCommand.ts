import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentsCompressionConfigurationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.DocumentsCompressionConfiguration> {
        const url = endpoints.databases.documentsCompression.documentsCompressionConfig;

        return this.query<Raven.Client.ServerWide.DocumentsCompressionConfiguration>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get documents compression configuration`, response.responseText, response.statusText))
    }
}

export = getDocumentsCompressionConfigurationCommand;
