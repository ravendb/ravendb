import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class migrateDatabaseCommand<T> extends commandBase {

    private constructor(private db: database,
        private dto: Raven.Server.Smuggler.Migration.MigrationConfiguration,
        private skipErrorReporting: boolean) {
        super();
    }

    execute(): JQueryPromise<T> {
        const url = endpoints.databases.smuggler.adminSmugglerMigrate;

        return this.post(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                if (this.skipErrorReporting) {
                    return;
                }

                this.reportError("Failed to migrate database", response.responseText, response.statusText);
            });
    }
    
    static migrate(db: database, dto: Raven.Server.Smuggler.Migration.MigrationConfiguration) {
        dto.InputConfiguration.Command = "export";
        return new migrateDatabaseCommand<operationIdDto>(db, dto, false);
    }
    
    static validateMigratorPath(db: database, fullPath: string) {
        const dto = {
            MigratorFullPath: fullPath,
            InputConfiguration: {
                Command: "validateMigratorPath"
            }
        } as Raven.Server.Smuggler.Migration.MigrationConfiguration; 
        return new migrateDatabaseCommand<void>(db, dto, true);
    }
    
    static getDatabaseNames(db: database, dto: Raven.Server.Smuggler.Migration.MigrationConfiguration) {
        dto.InputConfiguration.Command = "databases";
        return new migrateDatabaseCommand<{ Databases: Array<string> }>(db, dto, false);
    }
    
    static getCollections(db: database, dto: Raven.Server.Smuggler.Migration.MigrationConfiguration) {
        dto.InputConfiguration.Command = "collections";
        return new migrateDatabaseCommand<{ Collections: Array<string>; HasGridFS: boolean; }>(db, dto, false);
    }
}

export = migrateDatabaseCommand; 
