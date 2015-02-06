import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import savePeriodicExportSetupCommand = require("commands/savePeriodicExportSetupCommand");
import document = require("models/document");
import periodicExportSetup = require("models/periodicExportSetup");
import getGlobalPeriodicExportCommand = require("commands/getGlobalPeriodicExportCommand");
import appUrl = require("common/appUrl");
import configurationSettings = require("models/configurationSettings");
import getConfigurationSettingsCommand = require("commands/getConfigurationSettingsCommand");
import deleteLocalPeriodicExportSetupCommand = require("commands/deleteLocalPeriodicExportSetupCommand");

class periodicExport extends viewModelBase {

    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    globalBackupSetup = ko.observable<periodicExportSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;

    exportDisabled = ko.observable<boolean>(false);

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup);
        this.globalBackupSetup(new periodicExportSetup);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db))
                .done(() => {
                    this.updateExportDisabledFlag();
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        
        this.dirtyFlag = new ko.DirtyFlag([this.backupSetup, this.usingGlobal]);

        var self = this;
        this.isSaveEnabled = ko.computed(() => {
            var onDisk = self.backupSetup().onDiskExportEnabled();
            var remote = self.backupSetup().remoteUploadEnabled();
            var hasAnyOption = onDisk || remote;
            var isDirty = this.dirtyFlag().isDirty();
            return hasAnyOption && isDirty;
        });
    }

    fetchPeriodicExportSetup(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getGlobalPeriodicExportCommand(db)
            .execute()
            .done((result: configurationDocumentDto<periodicExportSetupDto>) => {
                this.backupSetup().fromDto(result.MergedDocument);
                this.hasGlobalValues(result.GlobalExists);
                this.usingGlobal(result.GlobalExists && !result.LocalExists);
                if (this.hasGlobalValues()) {
                    this.globalBackupSetup().fromDto(result.GlobalDocument);
                }
            })
            .always(() => deferred.resolve());
        return deferred;
    }

    fetchPeriodicExportAccountsSettings(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        var dbSettingsTask = new getDatabaseSettingsCommand(db)
            .execute()
            .done((document: document)=> { this.backupSetup().fromDatabaseSettingsDto(document.toDto(true)); })
            .always(() => deferred.resolve());


        var configTask = new getConfigurationSettingsCommand(db,
            ["Raven/AWSAccessKey", "Raven/AWSSecretKey", "Raven/AzureStorageAccount", "Raven/AzureStorageKey"])
            .execute()
            .done((result: configurationSettings) => {
                var awsAccess = result.results["Raven/AWSAccessKey"];
                var awsSecret = result.results["Raven/AWSSecretKey"];
                var azureAccess = result.results["Raven/AzureStorageAccount"];
                var azureSecret = result.results["Raven/AzureStorageKey"];
                this.globalBackupSetup().awsAccessKey(awsAccess.globalValue());
                this.globalBackupSetup().awsSecretKey(awsSecret.globalValue());
                this.globalBackupSetup().azureStorageAccount(azureAccess.globalValue());
                this.globalBackupSetup().azureStorageKey(azureSecret.globalValue());
            });

        return $.when(dbSettingsTask, configTask);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var task: JQueryPromise<any>;
            if (this.usingGlobal()) {
                task = new deleteLocalPeriodicExportSetupCommand(this.backupSetup(), db)
                    .execute();
            } else {
                task = new savePeriodicExportSetupCommand(this.backupSetup(), db).execute();
            }
            task.done((resultArray) => {
                var newEtag = resultArray[0].ETag;
                this.backupSetup().setEtag(newEtag);
                this.dirtyFlag().reset(); // Resync changes
                this.updateExportDisabledFlag();
            });
        }
    }

    useLocal() {
        this.usingGlobal(false);
    }

    useGlobal() {
        this.usingGlobal(true);
        this.backupSetup().copyFrom(this.globalBackupSetup());
    }

    updateExportDisabledFlag() {
        if (this.usingGlobal()) {
            this.exportDisabled(this.globalBackupSetup().disabled());
        } else {
            this.exportDisabled(this.backupSetup().disabled());
        }
    }
}

export = periodicExport; 