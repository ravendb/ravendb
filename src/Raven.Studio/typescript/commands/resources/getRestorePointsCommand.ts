import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRestorePointsCommand extends commandBase {

    private constructor(private path: string, private skipReportingError: boolean,
                        private connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType,
                        private s3Credentials?: Raven.Client.Documents.Operations.Backups.S3Settings) {
        super();
    }

    private preparePayload() {
        switch (this.connectionType) {
            case "Local":
                return {
                    FolderPath: this.path
                };
            case "S3":
                return this.s3Credentials;
        }
    }
    
    execute(): JQueryPromise<Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints> {
        const args = {
            type: this.connectionType
        } as any;
        
        const url = endpoints.global.adminDatabases.adminRestorePoints + this.urlEncodeArgs(args);
        
        return this.post(url, JSON.stringify(this.preparePayload()))
            .fail((response: JQueryXHR) => {
                if (this.skipReportingError) {
                    return;
                }

                this.reportError(`Failed to get restore points for path: ${this.path}`,
                    response.responseText,
                    response.statusText);
            });
    }
    
    static forServerLocal(path: string, skipReportingError: boolean) {
        return new getRestorePointsCommand(path, skipReportingError, "Local");
    }
    
    static forS3Backup(credentials: Raven.Client.Documents.Operations.Backups.S3Settings, skipReportingError: boolean) {
        return new getRestorePointsCommand("", skipReportingError, "S3", credentials);
    }
}

export = getRestorePointsCommand; 
