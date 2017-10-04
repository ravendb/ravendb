import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import savePeriodicExportSetupCommand = require("commands/maintenance/savePeriodicExportSetupCommand");
import document = require("models/database/documents/document");
import periodicExportSetup = require("models/database/documents/periodicExportSetup");
import getEffectivePeriodicExportCommand = require("commands/database/globalConfig/getEffectivePeriodicExportCommand");
import appUrl = require("common/appUrl");
import configurationSettings = require("models/database/globalConfig/configurationSettings");
import getConfigurationSettingsCommand = require("commands/database/globalConfig/getConfigurationSettingsCommand");
import deleteLocalPeriodicExportSetupCommand = require("commands/database/globalConfig/deleteLocalPeriodicExportSetupCommand");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");

type canEditSettingsDetails = { canEdit: boolean, canViaOverride: boolean };

class periodicExport extends viewModelBase {
    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    globalBackupSetup = ko.observable<periodicExportSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;
    exportDisabled = ko.observable<boolean>(false);

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);
    isForbidden = ko.observable<boolean>(false);
    useAlternativeEditMethod = ko.observable<boolean>(false);
    
    showOnDiskExportRow: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.activeDatabase.subscribe((db: database) => this.isForbidden(!db.isAdminCurrentTenant()));
        this.showOnDiskExportRow = ko.computed(() => {
            var localSetting = this.backupSetup() && this.backupSetup().onDiskExportEnabled();
            var globalSetting = this.hasGlobalValues() && this.globalBackupSetup().onDiskExportEnabled();
            return localSetting || globalSetting;
        });
    }

    attached() {
        super.attached();
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

        this.canEditSettings()
            .done((editSettings: canEditSettingsDetails) => {
                this.isForbidden(!editSettings.canEdit);

                this.useAlternativeEditMethod(editSettings.canViaOverride);

                if (editSettings.canEdit) {
                    var db = this.activeDatabase();

                    $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db, editSettings.canViaOverride))
                        .done(() => {
                            this.updateExportDisabledFlag();
                            deferred.resolve({ can: true });
                        })
                        .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
                } else {
                    deferred.resolve({ can: true });
                }
            });

        return deferred;
    }

    private canEditSettings(): JQueryPromise<canEditSettingsDetails> {
        var db = this.activeDatabase();
        if (db.isAdminCurrentTenant()) {
            return $.Deferred<canEditSettingsDetails>().resolve({ canEdit: true, canViaOverride: false });
        } else {
            // non-admin user - give him another chance by checking configuration endpoint
            var configTask = $.Deferred<canEditSettingsDetails>();

            new getConfigurationSettingsCommand(db, [/* just checking permissions */])
                .execute()
                .done(() => configTask.resolve({ canEdit: true, canViaOverride: true }))
                .fail((response: JQueryXHR) => {
                    if (response.status === 403) {
                        configTask.resolve({ canEdit: false, canViaOverride: false });
                    } else {
                        configTask.reject();
                    }
                });

            return configTask;
        }
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

    fetchPeriodicExportAccountsSettings(db: database, useAlternativeFetchMethod: boolean): JQueryPromise<any> {
        if (useAlternativeFetchMethod) {
            return this.getConfigurationSettings(db);
        } else {
            var task = $.Deferred();
            var dbSettingsTask = new getDatabaseSettingsCommand(db)
                .execute()
                .done((document: document) => { this.backupSetup().fromDatabaseSettingsDto(document.toDto(true)); });

            dbSettingsTask.then(() => {
                this.getConfigurationSettings(db)
                    .done(() => task.resolve());
            });

            return task;
        }
    }

    private getConfigurationSettings(db: database): JQueryPromise<configurationSettings> {
        return new getConfigurationSettingsCommand(db,
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
            });
    }


    saveChanges() {
        eventsCollector.default.reportEvent("periodic-export", "save");
        var db = this.activeDatabase();
        if (db) {
            var task: JQueryPromise<any>;
            if (this.usingGlobal()) {
                task = new deleteLocalPeriodicExportSetupCommand(this.backupSetup(), db)
                    .execute();
            } else {
                task = new savePeriodicExportSetupCommand(this.backupSetup(), db, false, this.useAlternativeEditMethod()).execute();
            }
            task.done((resultArray) => {
                // when no changes were made to database document server responds with NotModified, so resultArray[0] is undefined
                // as result etag doesn't change
                if (!this.useAlternativeEditMethod() && resultArray[0]) {
                    var newEtag = resultArray[0].ETag;
                    this.backupSetup().setEtag(newEtag);
                }
                
                this.backupSetup().resetDecryptionFailures();
                this.dirtyFlag().reset(); // Resync changes
                this.updateExportDisabledFlag();
            });
        }
    }

    useLocal() {
        eventsCollector.default.reportEvent("periodic-export", "use-local");
        this.usingGlobal(false);
    }

    useGlobal() {
        eventsCollector.default.reportEvent("periodic-export", "use-global");
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
