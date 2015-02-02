import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicExportSetupCommand = require("commands/getPeriodicExportSetupCommand");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import savePeriodicExportSetupCommand = require("commands/savePeriodicExportSetupCommand");
import document = require("models/document");
import periodicExportSetup = require("models/periodicExportSetup");
import appUrl = require("common/appUrl");

class periodicExport extends viewModelBase {

    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;
    backupStatusDirtyFlag = new ko.DirtyFlag([]);
    backupConfigDirtyFlag = new ko.DirtyFlag([]);

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db))
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('OU78CB');
        
        this.backupStatusDirtyFlag = new ko.DirtyFlag([this.backupSetup().disabled]);
        this.backupConfigDirtyFlag = new ko.DirtyFlag([this.backupSetup]);
        
        var self = this;
        this.isSaveEnabled = ko.computed(() => {
            var onDisk = self.backupSetup().onDiskExportEnabled();
            var remote = self.backupSetup().remoteUploadEnabled();
            var hasAnyOption = onDisk || remote;
            return (self.backupConfigDirtyFlag().isDirty() && hasAnyOption) &&
                (!self.backupSetup().disabled() || self.backupConfigDirtyFlag().isDirty());
        });

        this.dirtyFlag = new ko.DirtyFlag([this.isSaveEnabled]);
    }

    fetchPeriodicExportSetup(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getPeriodicExportSetupCommand(db)
            .execute()
            .done((result: periodicExportSetupDto) => this.backupSetup().fromDto(result) )
            .always(() => deferred.resolve());
        return deferred;
    }

    fetchPeriodicExportAccountsSettings(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getDatabaseSettingsCommand(db)
            .execute()
            .done((document: document)=> { this.backupSetup().fromDatabaseSettingsDto(document.toDto(true)); })
            .always(() => deferred.resolve());
        return deferred;
    }

    activatePeriodicExport() {
        var action: boolean = !this.backupSetup().disabled();
        this.backupSetup().disabled(action);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var saveTask = new savePeriodicExportSetupCommand(this.backupSetup(), db).execute();
            saveTask.done((resultArray) => {
                var newEtag = resultArray[0].ETag;
                this.backupSetup().setEtag(newEtag);
                this.backupStatusDirtyFlag().reset(); //Resync Changes
                this.backupConfigDirtyFlag().reset(); //Resync Changes
            });
        }
    }
}

export = periodicExport; 