import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class migrateSqlDatabaseCommand extends commandBase {
    
    constructor(private db: database, private dto: Raven.Server.SqlMigration.Model.MigrationRequest) {
          super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.sqlMigration.adminSqlMigrationImport;
        return this.post<operationIdDto>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to initialize SQL Migration operation`, response.responseText, response.statusText);
            });
    }
}

export = migrateSqlDatabaseCommand; 
