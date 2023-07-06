import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

class saveDatabaseStudioConfigurationCommand extends commandBase {
    
    private readonly dto: StudioConfiguration;
    private readonly db: database;

    constructor(dto: StudioConfiguration, db: database) {
        super();
        this.dto = dto;
        this.db = db;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminConfiguration.adminConfigurationStudio;
        return this.put<void>(url, JSON.stringify(this.dto), this.db, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(`Failed to save the database studio configuration`, response.responseText, response.statusText)) 
            .done(() => this.reportSuccess("Database studio configuration saved successfully"));
    }
}

export = saveDatabaseStudioConfigurationCommand;
