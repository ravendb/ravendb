import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import endpoints = require("endpoints");

class migrateDatabaseCommand extends commandBase {

    constructor(private db: database, private model: migrateDatabaseModel) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.smuggler.adminSmugglerMigrate;
        
        return this.post(url, JSON.stringify(this.model.toDto()), this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to migrate database", response.responseText, response.statusText));
    }
}

export = migrateDatabaseCommand; 
