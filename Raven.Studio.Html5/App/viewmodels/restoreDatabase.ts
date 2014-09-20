import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");

class restoreDatabase extends viewModelBase {

    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>('');
    databaseLocation = ko.observable<string>();
    databaseName = ko.observable<string>();
    nameCustomValidityError: KnockoutComputed<string>;

    restoreStatusMessages = ko.observableArray<string>();
    isBusy = ko.observable<boolean>();

    constructor() {
        super();

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newDatabaseName = this.databaseName();
            var foundDb = shell.databases.first((db: database) => newDatabaseName == db.name);

            if (!!foundDb && newDatabaseName.length > 0) {
                errorMessage = "Database name already exists!";
            }

            return errorMessage;
        });
    }

    canActivate(args): any {
        return true;
    }

    startRestore() {
        this.isBusy(true);

        var restoreDatabaseDto: databaseRestoreRequestDto = {
            BackupLocation: this.backupLocation(),
            DatabaseLocation: this.databaseLocation(),
            DatabaseName: this.databaseName()
        };
        var updateRestoreStatus = (newRestoreStatus: restoreStatusDto) => {
            this.restoreStatusMessages(newRestoreStatus.Messages);
            this.isBusy(!!newRestoreStatus.IsRunning);
        };

        require(["commands/startRestoreCommand"], startRestoreCommand => {
            new startRestoreCommand(this.defrag(), restoreDatabaseDto, updateRestoreStatus)
                .execute();
        });
    }
}

export = restoreDatabase;  