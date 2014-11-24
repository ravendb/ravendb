import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import monitorRestoreCommand = require("commands/monitorRestoreCommand");

class restoreDatabase extends viewModelBase {

    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>('');
    databaseLocation = ko.observable<string>();
    databaseName = ko.observable<string>();
    nameCustomValidityError: KnockoutComputed<string>;

    restoreStatusMessages = ko.observableArray<string>();
    isBusy = ko.observable<boolean>();
    anotherRestoreInProgres = ko.observable<boolean>(false);
    keepDown = ko.observable<boolean>(false);

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
        this.isBusy(true);
        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();
        var self = this;

        new getDocumentWithMetadataCommand("Raven/Restore/InProgress", db, true).execute()
            .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }))
            .done((result) => {
                if (result) {
                    // looks like another restore is in progress
                    this.anotherRestoreInProgres(true);
                    new monitorRestoreCommand($.Deferred(), this.updateRestoreStatus.bind(self))
                        .execute()
                        .always(() => {
                            $("#rawLogsContainer").resize();
                            this.anotherRestoreInProgres(false);
                        });

                } else {
                    this.isBusy(false);
                }
                deferred.resolve({ can: true })
            });

        return deferred;
    }

    private updateRestoreStatus(newRestoreStatus: restoreStatusDto) {
        this.restoreStatusMessages(newRestoreStatus.Messages);
        if (this.keepDown()) {
            var logsPre = document.getElementById('restoreLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
        this.isBusy(!!newRestoreStatus.IsRunning);
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown() == true) {
            var logsPre = document.getElementById('restoreLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
    }

    startRestore() {
        this.isBusy(true);
        var self = this;

        var restoreDatabaseDto: databaseRestoreRequestDto = {
            BackupLocation: this.backupLocation(),
            DatabaseLocation: this.databaseLocation(),
            DatabaseName: this.databaseName()
        };

        require(["commands/startRestoreCommand"], startRestoreCommand => {
            new startRestoreCommand(this.defrag(), restoreDatabaseDto, self.updateRestoreStatus.bind(self))
                .execute();
        });
    }
}

export = restoreDatabase;  