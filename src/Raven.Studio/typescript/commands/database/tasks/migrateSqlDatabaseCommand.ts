import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class migrateSqlDatabaseCommand extends commandBase {
    
    constructor(private db: database, private dto: Raven.Server.SqlMigration.Model.MigrationRequest) {
          super();
    }

    execute(): JQueryPromise<void> { //TODO: it should return operation id
        const url = endpoints.databases.sqlMigration.adminSqlMigrationImport;
        return this.post<void>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to migrate SQL database`, response.responseText, response.statusText);
            });
    }
}

export = migrateSqlDatabaseCommand; 
