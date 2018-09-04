import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class validateOfflineMigration extends commandBase {

    constructor(private mode: "dataDir" | "migratorPath", private pathToTest: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation> {
        const args = {
            mode: this.mode,
            path: this.pathToTest
        };
        const url = endpoints.global.studioTasks.studioTasksOfflineMigrationTest;
        
        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation>(url, args);
    }
    
    static validateDataDir(dataDir: string): validateOfflineMigration {
        return new validateOfflineMigration("dataDir", dataDir);
    }
    
    static validateMigratorPath(migratorPath: string): validateOfflineMigration {
        return new validateOfflineMigration("migratorPath", migratorPath);
    }
}

export = validateOfflineMigration; 
