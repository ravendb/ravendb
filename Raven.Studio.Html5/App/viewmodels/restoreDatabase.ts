import viewModelBase = require("viewmodels/viewModelBase");

class restoreDatabase extends viewModelBase {

    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>('');
    databaseLocation = ko.observable<string>();
    databaseName = ko.observable<string>();

    restoreStatusMessages = ko.observableArray<string>();
    isBusy = ko.observable<boolean>();

    canActivate(args): any {
        return true;
    }

    startRestore() {
        var restoreDatabaseDto: restoreRequestDto = {
            RestoreLocation: this.backupLocation(),
            DatabaseLocation: this.databaseLocation(),
            DatabaseName: this.databaseName()
        };
        var updateRestoreStatus = (newRestoreStatus: restoreStatusDto) => {
            this.restoreStatusMessages(newRestoreStatus.Messages);
            this.isBusy(!!newRestoreStatus.IsRunning);
        };

        require(["commands/startRestoreCommand"], startRestoreCommand => {
            this.isBusy(true);
            new startRestoreCommand(this.defrag(), restoreDatabaseDto, updateRestoreStatus)
                .execute();
        });
    }
}

export = restoreDatabase;  