import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

class saveClientConfigurationCommand extends commandBase {
    private dto: ClientConfiguration;
    private db: database;

    constructor(dto: ClientConfiguration,  db: database) {
        super();
        this.dto = dto;
        this.db = db;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminConfiguration.adminConfigurationClient;
        return this.put<void>(url, JSON.stringify(this.dto), this.db, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(`Failed to save client configuration`, response.responseText, response.statusText)) 
            .done(() => this.reportSuccess("Saved client configuration"));
    }
    
}

export = saveClientConfigurationCommand;
