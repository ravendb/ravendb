import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");

class backupDatabase extends viewModelBase {

    backupLocation = ko.observable<string>('');
    backupStatusMessages = ko.observableArray<backupMessageDto>();
    isBusy = ko.observable<boolean>();
    databaseNames: KnockoutComputed<Array<string>>;

    constructor() {
        super();

        this.databaseNames = ko.computed(() => {
            return shell.databases().map((db: database) => db.name);
        });
    }

    canActivate(args): any {
        return true;
    }

    startBackup() {
        var updateBackupStatus = (newBackupStatus: backupStatusDto) => {
            this.backupStatusMessages(newBackupStatus.Messages);
            this.isBusy(!!newBackupStatus.IsRunning);
        };
        this.isBusy(true);

        require(["commands/backupDatabaseCommand"], backupDatabaseCommand => {
            new backupDatabaseCommand(this.activeDatabase(), this.backupLocation(), updateBackupStatus)
                .execute()
                .always(() => this.isBusy(false));
        });
    }
}

export = backupDatabase;