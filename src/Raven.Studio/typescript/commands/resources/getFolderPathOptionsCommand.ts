import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getFolderPathOptionsCommand extends commandBase {

    constructor(private inputPath: string, private isBackupFolder: boolean = false, private connectionType: string = "Local") {
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
}

export = getFolderPathOptionsCommand;
