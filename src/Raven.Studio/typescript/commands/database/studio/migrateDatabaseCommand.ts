import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class migrateDatabaseCommand<T> extends commandBase {

    constructor(private db: database,
        private dto: Raven.Server.Smuggler.Migration.MigrationConfiguration,
        private skipErrorReporting: boolean) {
        super();
    }

    execute(): JQueryPromise<T> {
        const url = endpoints.databases.smuggler.smugglerMigrate;

        return this.post(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                if (this.skipErrorReporting) {
                    return;
                }

                this.reportError("Failed to migrate database", response.responseText, response.statusText);
            });
    }
}

export = migrateDatabaseCommand; 
