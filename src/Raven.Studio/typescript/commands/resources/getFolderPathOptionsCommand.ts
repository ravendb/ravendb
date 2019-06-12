import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getFolderPathOptionsCommand extends commandBase {

    //TODO: add extra parameter when cloud
    private constructor(private inputPath: string, private isBackupFolder: boolean = false, private connectionType: restoreSource) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        const args = {
            path: this.inputPath ? this.inputPath : "",
            connectionType: this.connectionType,
            backupFolder: this.isBackupFolder
        };

        const url = endpoints.global.studioTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.FolderPathOptions>(url, null, null)
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
    
    static forS3Backup(inputPath: string) {
        return new getFolderPathOptionsCommand(inputPath, false, "cloud"); //TODO: pass s3 credentials
    }
}

export = getFolderPathOptionsCommand;
