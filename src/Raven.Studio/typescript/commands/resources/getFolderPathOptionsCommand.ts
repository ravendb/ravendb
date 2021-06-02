import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getFolderPathOptionsCommand extends commandBase {

    private constructor(
        private db: database, 
        private inputPath: string, 
        private isBackupFolder: boolean = false, 
        private connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType, 
        private credentials?: Raven.Client.Documents.Operations.Backups.BackupSettings ) {
        super();
    }

    private preparePayload() {
        switch (this.connectionType) {
            case "Local":
                return undefined;
            default:
                return JSON.stringify(this.credentials);
        }
    }
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        const args = {
            type: this.connectionType
        } as any;
        
        if (this.connectionType === "Local") {
            args.path = this.inputPath ? this.inputPath : "";
            args.backupFolder = this.isBackupFolder;
        }

        const url = this.db 
            ? endpoints.databases.studioDatabaseTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args)
            : endpoints.global.studioTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args);
        
        return this.post<Raven.Server.Web.Studio.FolderPathOptions>(url, this.preparePayload(), this.db);
    }
    
    static forServerLocal(inputPath: string, isBackupFolder: boolean, db: database = null) {
        return new getFolderPathOptionsCommand(db, inputPath, isBackupFolder, "Local");
    }

    static forCloudBackup(credentials: Raven.Client.Documents.Operations.Backups.BackupSettings,type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType) {
        return new getFolderPathOptionsCommand(null, null, false, type, credentials);  
    }
}

export = getFolderPathOptionsCommand;
