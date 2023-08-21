import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveDataArchivalConfigurationCommand extends commandBase {
    private readonly db: database;
    private readonly configuration: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
    

    constructor(db: database, configuration: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration) {
        super();
        this.db = db;
        this.configuration = configuration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.dataArchival.adminDataArchivalConfig;
        const args = ko.toJSON(this.configuration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save data archival configuration", response.responseText, response.statusText));

    }
}

export = saveDataArchivalConfigurationCommand;
