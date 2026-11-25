import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class migrateRavenDbDatabaseCommand extends commandBase {

    constructor(private db: database | string, private dto: Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.smuggler.adminSmugglerMigrateRavendb;
        
        return this.post(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to migrate database", response.responseText, response.statusText));
    }
}

export = migrateRavenDbDatabaseCommand; 
