import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;

class saveRefreshConfigurationCommand extends commandBase {
    
    private readonly db: database;
    private readonly refreshConfiguration: RefreshConfiguration;
    
    constructor(db: database, refreshConfiguration: RefreshConfiguration) {
        super();
        this.db = db;
        this.refreshConfiguration = refreshConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.refresh.adminRefreshConfig;
        const args = ko.toJSON(this.refreshConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save refresh configuration", response.responseText, response.statusText));
    }
}

export = saveRefreshConfigurationCommand;
