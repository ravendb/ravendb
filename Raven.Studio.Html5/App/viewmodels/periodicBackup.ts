import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicBackupSetupCommand = require("commands/getPeriodicBackupSetupCommand");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import periodicBackupSetup = require("models/periodicBackupSetup");
import savePeriodicBackupSetupCommand = require("commands/savePeriodicBackupSetupCommand");
import appUrl = require("common/appUrl");

class periodicBackup extends viewModelBase {

    backupSetup = ko.observable<periodicBackupSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;
    backupStatusDirtyFlag = new ko.DirtyFlag([]);
    backupConfigDirtyFlag = new ko.DirtyFlag([]);

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
        
        this.backupStatusDirtyFlag = new ko.DirtyFlag([this.backupSetup().activated]);
        this.backupConfigDirtyFlag = new ko.DirtyFlag([this.backupSetup]);
        
        var self = this;
        this.isSaveEnabled = ko.computed(function () {
            return (self.backupConfigDirtyFlag().isDirty()) &&
                    (self.backupSetup().activated() || (!self.backupSetup().activated() && self.backupStatusDirtyFlag().isDirty()));
        });

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.isSaveEnabled]);
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
        var action: boolean = !this.backupSetup().activated();
        this.backupSetup().activated(action);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var saveTask = new savePeriodicBackupSetupCommand(this.backupSetup(), db).execute();
            saveTask.done(() => {
                this.backupStatusDirtyFlag().reset(); //Resync Changes
                this.backupConfigDirtyFlag().reset(); //Resync Changes
            });
        }
    }
}

export = periodicBackup; 