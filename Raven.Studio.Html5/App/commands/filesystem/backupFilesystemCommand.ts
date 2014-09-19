import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import document = require("models/document");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");

class backupFilesystemCommand extends commandBase {

    constructor(private fs: filesystem, private backupLocation: string, private updateBackupStatus: (backupStatusDto) => void, private incremental: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var result = $.Deferred();

        var args: backupRequestDto = {
                    BackupLocation: this.backupLocation,
                    DatabaseDocument: null
                };
        this.post('/admin/fs/backup?incremental=' + this.incremental, JSON.stringify(args), this.fs, { dataType: 'text' })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to create backup!", response.responseText, response.statusText);
                result.reject();
            })
            .done(() => this.getBackupStatus(result));

        return result;
    }

    private getBackupStatus(result: JQueryDeferred<any>) {
        new getDocumentWithMetadataCommand("Raven/FileSystem/Backup/Status/" + this.fs.name, appUrl.getSystemDatabase())
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch backup status!", response.responseText, response.statusText);
                result.reject();
            })
            .done((backupStatus: backupStatusDto)=> {
                this.updateBackupStatus(backupStatus);
                if (backupStatus.IsRunning) {
                    setTimeout(() => this.getBackupStatus(result), 1000);
                } else {
                    this.reportSuccess("Filesystem backup was successfully created!");
                    result.resolve();
                }
            });
    }
}

export = backupFilesystemCommand;