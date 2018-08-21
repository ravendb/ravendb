import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import migrateRavenDbDatabaseModel = require("models/database/tasks/migrateRavenDbDatabaseModel");
import endpoints = require("endpoints");

class migrateRavenDbDatabaseCommand extends commandBase {

    constructor(private db: database, private model: migrateRavenDbDatabaseModel) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.smuggler.adminSmugglerMigrateRavendb;
        
        return this.post(url, JSON.stringify(this.model.toDto()), this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to migrate database", response.responseText, response.statusText));
    }
}

export = migrateRavenDbDatabaseCommand; 
