import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicBackupSetupCommand = require("commands/getPeriodicBackupSetupCommand");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import periodicBackupSetup = require("models/periodicBackupSetup");
import savePeriodicBackupSetupCommand = require("commands/savePeriodicBackupSetupCommand");
import appUrl = require("common/appUrl");

class periodicBackup extends viewModelBase {

    backupSetup = ko.observable<periodicBackupSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;
    static containerId = "#periodicBackupContainer";
    private form: JQuery;

    canActivate(args: any): any {
        super.canActivate(args);

        this.backupSetup(new periodicBackupSetup);
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchPeriodicBackupSetup(db), this.fetchPeriodicBackupAccountsSettings(db))
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.isSaveEnabled = ko.computed(function() {
            return true;
        });
        //viewModelBase.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    attached() {
        this.form = $("#save-periodic-backup-form");
    }

    fetchPeriodicBackupSetup(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getPeriodicBackupSetupCommand(db)
            .execute()
            .done((result: periodicBackupSetupDto) => this.backupSetup().fromDto(result) )
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchPeriodicBackupAccountsSettings(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getDatabaseSettingsCommand(db)
            .execute()
            .done(document => this.backupSetup().fromDatabaseSettingsDto(document.toDto()) )
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    activatePeriodicBackup() {
        this.backupSetup().activated(true);
    }

    saveChanges() {
        //if ((<any>this.form[0]).checkValidity() === true) {
            var db = this.activeDatabase();
            if (db) {
                new savePeriodicBackupSetupCommand(this.backupSetup(), db).execute();
            }
        //}
    }
}

export = periodicBackup; 