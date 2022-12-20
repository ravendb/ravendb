import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabasesForStudioCommand extends commandBase {

    execute(): JQueryPromise<StudioDatabasesResponse> {
        const url = endpoints.global.studioDatabases.studioTasksDatabases;

        return this.query<StudioDatabasesResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to load databases", response.responseText, response.statusText));
    }
}

export = getDatabasesForStudioCommand;
