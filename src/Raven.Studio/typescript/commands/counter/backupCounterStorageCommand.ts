import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");

class backupFilesystemCommand extends commandBase {

    constructor(private cs: counterStorage, private backupLocation: string, private updateBackupStatus: (status: backupStatusDto) => void, private incremental: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var result = $.Deferred();

        var args: backupRequestDto = {
                    BackupLocation: this.backupLocation,
                    DatabaseDocument: null
                };
        this.post('/admin/backup?incremental=' + this.incremental, JSON.stringify(args), this.cs, { dataType: 'text' })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to create backup!", response.responseText, response.statusText);
                result.reject();
            })
            .done(() => this.getBackupStatus(result));

        return result;
    }

    private getBackupStatus(result: JQueryDeferred<any>) {
        /*new getConfigurationByKeyCommand(this.cs, "Raven/Backup/Status")
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch backup status!", response.responseText, response.statusText);
                result.reject();
            })
            .done((backupStatusAsString: string) => {
                var backupStatus: backupStatusDto = JSON.parse(backupStatusAsString);
                this.updateBackupStatus(backupStatus);
                if (backupStatus.IsRunning) {
                    setTimeout(() => this.getBackupStatus(result), 1000);
                } else {
                    this.reportSuccess("Counter Storage backup was successfully created!");
                    result.resolve();
                }
            });*/
    }
}

export = backupFilesystemCommand;
