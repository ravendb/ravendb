import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getFolderPathOptionsCommand extends commandBase {

    private constructor(private inputPath: string, private isBackupFolder: boolean = false, private connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType, 
                        private s3Credentials?: Raven.Client.Documents.Operations.Backups.S3Settings) {
        super();
    }

    private preparePayload() {
        switch (this.connectionType) {
            case "Local":
                return undefined;
            case "S3":
                return JSON.stringify(this.s3Credentials);
                
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
        
        return this.post<Raven.Server.Web.Studio.FolderPathOptions>(url, this.preparePayload(), null)
            .fail((response: JQueryXHR) => {
                if (response.status === 403) {
                    return;
                }

                this.reportError("Failed to get the folder path options", response.responseText, response.statusText);
            });
    }
    
    static forServerLocal(inputPath: string, isBackupFolder: boolean) {
        return new getFolderPathOptionsCommand(inputPath, isBackupFolder, "Local");
    }
    
    static forS3Backup(credentials: Raven.Client.Documents.Operations.Backups.S3Settings) {
        return new getFolderPathOptionsCommand(null, false, "S3", credentials);  
    }
}

export = getFolderPathOptionsCommand;
