import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import savePeriodicExportSetupCommand = require("commands/maintenance/savePeriodicExportSetupCommand");
import document = require("models/database/documents/document");
import periodicExportSetup = require("models/database/documents/periodicExportSetup");
import appUrl = require("common/appUrl");
import configurationSettings = require("models/database/globalConfig/configurationSettings");
import getConfigurationSettingsCommand = require("commands/database/globalConfig/getConfigurationSettingsCommand");
import deleteLocalPeriodicExportSetupCommand = require("commands/database/globalConfig/deleteLocalPeriodicExportSetupCommand");
import database = require("models/resources/database");
import getPeriodicExportSetupCommand = require("commands/database/documents/getPeriodicExportSetupCommand");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");

class periodicExport extends viewModelBase {
    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;
    exportDisabled = ko.observable<boolean>(false);

    isForbidden = ko.observable<boolean>(false);
    
    showOnDiskExportRow: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.activeDatabase.subscribe((db: database) => this.isForbidden(!db.isAdminCurrentTenant()));
        this.showOnDiskExportRow = ko.computed(() => this.backupSetup() && this.backupSetup().onDiskExportEnabled());
    }

    attached() {
        super.attached();
        var content = "Could not decrypt the access settings, if you are running on IIS, make sure that load user profile is set to true. " +
            "Alternatively this can happen when the server was started using different account than when the settings were created.<br />" +
            "Reenter your settings and click save.";

        $("#awsDecryptFailureSpan").popover({
            html: true,
            trigger: "hover",
            container: "body",
            content: content
        });

        $("#azureDecryptFailureSpan").popover({
            html: true,
            trigger: "hover",
            container: "body",
            content: content
        });
    }

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup);

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

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("OU78CB");
        
        this.dirtyFlag = new ko.DirtyFlag([this.backupSetup]);

        var self = this;
        this.isSaveEnabled = ko.computed(() => {
            var onDisk = self.backupSetup().onDiskExportEnabled();
            var remote = self.backupSetup().remoteUploadEnabled();
            var hasAnyOption = onDisk || remote;
            var isDirty = this.dirtyFlag().isDirty();
            return hasAnyOption && isDirty;
        });
    }

    fetchPeriodicExportSetup(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getPeriodicExportSetupCommand(db)
            .execute()
            .done((result: Raven.Server.Documents.PeriodicExport.PeriodicExportConfiguration) => {
                this.backupSetup().fromDto(result);
            })
            .always(() => deferred.resolve());
        return deferred;
    }

    fetchPeriodicExportAccountsSettings(db: database): JQueryPromise<any> {
        var task = $.Deferred();
        var dbSettingsTask = new getDatabaseSettingsCommand(db)
            .execute()
            .done((document: document) => {
                 this.backupSetup().fromDatabaseSettingsDto(document.toDto(false));
            });

        dbSettingsTask.then(() => {
            task.resolve(); //TODO: delete me!
            /* TODO add support for remote export
            new getConfigurationSettingsCommand(db,
                ["Raven/AWSAccessKey", "Raven/AWSSecretKey", "Raven/AzureStorageAccount", "Raven/AzureStorageKey"])
                .execute()
                .done((result: configurationSettings) => {
                    var awsAccess = result.results["Raven/AWSAccessKey"];
                    var awsSecret = result.results["Raven/AWSSecretKey"];
                    var azureAccess = result.results["Raven/AzureStorageAccount"];
                    var azureSecret = result.results["Raven/AzureStorageKey"];
                    this.backupSetup().awsAccessKey(awsAccess.effectiveValue());
                    this.backupSetup().awsSecretKey(awsSecret.effectiveValue());
                    this.backupSetup().azureStorageAccount(azureAccess.effectiveValue());
                    this.backupSetup().azureStorageKey(azureSecret.effectiveValue());
                    task.resolve();
            });*/
        });
        return task;
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {

            const periodicExportConfig = this.backupSetup().toDto();

            new saveDocumentCommand("Raven/PeriodicExport/Configuration",
                    new document(periodicExportConfig),
                    this.activeDatabase())
                .execute()
                .done(() => {
                    this.backupSetup().resetDecryptionFailures();
                    this.dirtyFlag().reset();
                    this.updateExportDisabledFlag();
                });
            /* TODO:
            var task: JQueryPromise<any>;
            task = new savePeriodicExportSetupCommand(this.backupSetup(), db).execute();
            task.done((resultArray) => {
                var newEtag = resultArray[0].ETag;
                this.backupSetup().setEtag(newEtag);
                this.backupSetup().resetDecryptionFailures();
                this.dirtyFlag().reset(); // Resync changes
                this.updateExportDisabledFlag();
            });*/
        }
    }

    updateExportDisabledFlag() {
        this.exportDisabled(!this.backupSetup().active());
    }
}

export = periodicExport; 
