import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getFolderPathOptionsCommand extends commandBase {

    private constructor(private inputPath: string, private isBackupFolder: boolean = false, private connectionType: restoreSource, 
                        private s3Credentials?: Raven.Client.Documents.Operations.Backups.S3Settings) {
        super();
    }

    private mapConnectionType(): Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType {
        switch (this.connectionType) {
            case "serverLocal":
                return "Local";
            case "cloud":
                return "S3";
        }
    }
    
    private preparePayload() {
        switch (this.connectionType) {
            case "serverLocal":
                return null;
            case "cloud":
                return this.s3Credentials;
                
        }
    }
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        const args = {
            type: this.mapConnectionType()
        } as any;
        
        if (this.connectionType === "serverLocal") {
            args.path = this.inputPath ? this.inputPath : "";
            args.backupFolder = this.isBackupFolder;
        }

        const url = endpoints.global.studioTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args);
        
        return this.post<Raven.Server.Web.Studio.FolderPathOptions>(url, JSON.stringify(this.preparePayload()), null)
            .fail((response: JQueryXHR) => {
                if (response.status === 403) {
                    return;
                }

                this.reportError("Failed to get the folder path options", response.responseText, response.statusText);
            });
    }
    
    static forServerLocal(inputPath: string, isBackupFolder: boolean) {
        return new getFolderPathOptionsCommand(inputPath, isBackupFolder, "serverLocal");
    }
    
    static forS3Backup(credentials: Raven.Client.Documents.Operations.Backups.S3Settings) {
        return new getFolderPathOptionsCommand(null, false, "cloud", credentials);  
    }
}

export = getFolderPathOptionsCommand;
