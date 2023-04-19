import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class restartDatabaseCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const url = endpoints.databases.studioDatabaseTasks.adminStudioTasksRestart;

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to restart the database", response.responseText, response.statusText));
    }

}

export = restartDatabaseCommand;  
