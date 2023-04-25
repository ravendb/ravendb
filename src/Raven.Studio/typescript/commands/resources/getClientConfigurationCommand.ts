import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getClientConfigurationCommand extends commandBase {
    private db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.ClientConfiguration> {
        const args = {
            inherit: false
        };
        const url = endpoints.databases.configuration.configurationClient + this.urlEncodeArgs(args);
        return this.query<Raven.Client.Documents.Operations.Configuration.ClientConfiguration>(url, null, this.db, x => x.Configuration)
            .fail((response: JQueryXHR) => this.reportError(`Failed to load client configuration`, response.responseText, response.statusText)) 
    }
    
}

export = getClientConfigurationCommand;
