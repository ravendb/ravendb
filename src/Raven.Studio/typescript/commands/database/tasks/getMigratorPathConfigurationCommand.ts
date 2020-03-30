import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getMigratorPathConfigurationCommand extends commandBase {

    execute(): JQueryPromise<Configuration> {
        const url = endpoints.global.studioTasks.studioTasksAdminMigratorPath;
       
        return this.query<Configuration>(url, null)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to get information about Migrator Path", response.responseText, response.statusText));
    }
}

export = getMigratorPathConfigurationCommand;
