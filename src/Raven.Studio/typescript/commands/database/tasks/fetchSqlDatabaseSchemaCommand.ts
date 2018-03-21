import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class fetchSqlDatabaseSchemaCommand extends commandBase {
    
    constructor(private db: database, private dto: Raven.Server.SqlMigration.Model.SourceSqlDatabase) {
          super();
    }

    execute(): JQueryPromise<Raven.Server.SqlMigration.Schema.DatabaseSchema> {
        const url = endpoints.databases.sqlMigration.adminSqlMigrationSchema;
        return this.post<Raven.Server.SqlMigration.Schema.DatabaseSchema>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get database schema`, response.responseText, response.statusText);
            });
    }
}

export = fetchSqlDatabaseSchemaCommand; 
