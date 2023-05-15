import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";

class restartDatabaseCommand extends commandBase {
    private db: DatabaseSharedInfo;
    private location: databaseLocationSpecifier;

    constructor(db: DatabaseSharedInfo, location: databaseLocationSpecifier) {
        super();
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const url = endpoints.databases.studioDatabaseTasks.adminStudioTasksRestart + this.urlEncodeArgs(this.location);

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to restart the database", response.responseText, response.statusText));
    }
}

export = restartDatabaseCommand;  
