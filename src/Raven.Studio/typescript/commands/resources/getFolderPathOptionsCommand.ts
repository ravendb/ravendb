import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getFolderPathOptionsCommand extends commandBase {

    private constructor(private inputPath: string, 
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

        const url = endpoints.global.studioTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args);
        
        return this.post<Raven.Server.Web.Studio.FolderPathOptions>(url, this.preparePayload(), null);
    }
    
    static forServerLocal(inputPath: string, isBackupFolder: boolean) {
        return new getFolderPathOptionsCommand(inputPath, isBackupFolder, "Local");
    }

    static forCloudBackup(credentials: Raven.Client.Documents.Operations.Backups.BackupSettings,type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType) {
        return new getFolderPathOptionsCommand(null, false, type, credentials);  
    }
}

export = getFolderPathOptionsCommand;
