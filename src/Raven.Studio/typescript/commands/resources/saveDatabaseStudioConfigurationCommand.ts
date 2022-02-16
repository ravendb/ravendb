import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class saveDatabaseStudioConfigurationCommand extends commandBase {
    
    constructor(private dto: Raven.Client.Documents.Operations.Configuration.StudioConfiguration, private db: database) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminConfiguration.adminConfigurationStudio;
        return this.put<void>(url, JSON.stringify(this.dto), this.db, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(`Failed to save the database studio configuration`, response.responseText, response.statusText)) 
            .done(() => this.reportSuccess("Database studio configuration saved successfully"));
    }
}

export = saveDatabaseStudioConfigurationCommand;
