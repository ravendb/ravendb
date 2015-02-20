import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import savePeriodicExportSetupCommand = require("commands/savePeriodicExportSetupCommand");
import document = require("models/document");
import periodicExportSetup = require("models/periodicExportSetup");
import getEffectivePeriodicExportCommand = require("commands/getEffectivePeriodicExportCommand");
import appUrl = require("common/appUrl");
import configurationSettings = require("models/configurationSettings");
import getConfigurationSettingsCommand = require("commands/getConfigurationSettingsCommand");
import deleteLocalPeriodicExportSetupCommand = require("commands/deleteLocalPeriodicExportSetupCommand");
import database = require("models/database");

class periodicExport extends viewModelBase {
    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    globalBackupSetup = ko.observable<periodicExportSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;
    exportDisabled = ko.observable<boolean>(false);

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);
    isForbidden = ko.observable<boolean>(false);
    

    constructor() {
        super();
        this.activeDatabase.subscribe((db: database) => this.isForbidden(!db.isAdminCurrentTenant()));
    }

    attached() {
        var content = "Could not decrypt the access settings, if you are running on IIS, make sure that load user profile is set to true. " +
            "Alternatively this can happen when the server was started using different account than when the settings were created.<br />" +
            "Reenter your settings and click save.";

        $("#awsDecryptFailureSpan").popover({
            html: true,
            trigger: "hover",
            container: $("body"),
            content: content
        });

        $("#azureDecryptFailureSpan").popover({
            html: true,
            trigger: "hover",
            container: $("body"),
            content: content
        });
    }

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup);
        this.globalBackupSetup(new periodicExportSetup);

        var deferred = $.Deferred();

        var db = this.activeDatabase();
        this.isForbidden(!db.isAdminCurrentTenant());
        if (db.isAdminCurrentTenant()) {
            $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db))
                .done(() => {
                    this.updateExportDisabledFlag();
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        } else {
            deferred.resolve({ can: true });
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("OU78CB");
        
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
        new getEffectivePeriodicExportCommand(db)
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
        var task = $.Deferred();
        var dbSettingsTask = new getDatabaseSettingsCommand(db)
            .execute()
            .done((document: document) => { this.backupSetup().fromDatabaseSettingsDto(document.toDto(true)); });

        dbSettingsTask.then(() => {
            new getConfigurationSettingsCommand(db,
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
                    this.backupSetup().awsAccessKey(awsAccess.effectiveValue());
                    this.backupSetup().awsSecretKey(awsSecret.effectiveValue());
                    this.backupSetup().azureStorageAccount(azureAccess.effectiveValue());
                    this.backupSetup().azureStorageKey(azureSecret.effectiveValue());
                    task.resolve();
            });
        });
        return task;
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
                this.backupSetup().resetDecryptionFailures();
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