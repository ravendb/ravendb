import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexDefaultsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioDatabaseTasksHandler.IndexDefaults> {
        const url = endpoints.databases.studioDatabaseTasks.studioTasksIndexesConfigurationDefaults;
        return this.query<Raven.Server.Web.Studio.StudioDatabaseTasksHandler.IndexDefaults>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get index defaults!", response.responseText, response.statusText);
            });
    }
}

export = getIndexDefaultsCommand;
