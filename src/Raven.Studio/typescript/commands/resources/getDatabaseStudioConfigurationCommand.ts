import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getDatabaseStudioConfigurationCommand extends commandBase {
    
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationStudio;
        const loadTask = $.Deferred<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>();

        this.query<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>(url, null, this.db)
            .done(dto => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                if (response.status !== 404) {
                    this.reportError(`Failed to load the studio database configuration`, response.responseText, response.statusText);
                    loadTask.reject(response);
                } else {
                    loadTask.resolve(null);
                }
            });

        return loadTask;
    }
}

export = getDatabaseStudioConfigurationCommand;
