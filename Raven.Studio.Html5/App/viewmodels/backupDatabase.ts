import backupDatabaseCommand = require("commands/backupDatabaseCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class backupDatabase extends viewModelBase {

    backupLocation = ko.observable<string>("C:\\path-to-your-backup-folder");
    backupStatusMessages = ko.observableArray<backupMessageDto>();
    isBusy = ko.observable<boolean>();

    startBackup() {
        var updateBackupStatus = (newBackupStatus: backupStatusDto) => {
            this.backupStatusMessages(newBackupStatus.Messages);
            this.isBusy(!!newBackupStatus.IsRunning);
        };
        this.isBusy(true);
        new backupDatabaseCommand(this.activeDatabase(), this.backupLocation(), updateBackupStatus)
            .execute()
            .always(() => this.isBusy(false));
    }

}

export = backupDatabase;