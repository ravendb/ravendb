import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class restartDatabaseCommand extends commandBase {
    private databaseName: string;
    private location: databaseLocationSpecifier;

    constructor(databaseName: string, location: databaseLocationSpecifier) {
        super();
        this.databaseName = databaseName;
        this.location = location;
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const url = endpoints.databases.studioDatabaseTasks.adminStudioTasksRestart + this.urlEncodeArgs(this.location);

        return this.post(url, null, this.databaseName, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to restart the database", response.responseText, response.statusText));
    }
}

export = restartDatabaseCommand;  
