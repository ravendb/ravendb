import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class validateOfflineMigration extends commandBase {

    private mode: "dataDir" | "migratorPath";

    private pathToTest: string;

    constructor(mode: "dataDir" | "migratorPath", pathToTest: string) {
        super();
        this.pathToTest = pathToTest;
        this.mode = mode;
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation> {
        const args = {
            mode: this.mode,
            path: this.pathToTest
        };
        const url = endpoints.global.studioTasks.adminStudioTasksOfflineMigrationTest;
        
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
