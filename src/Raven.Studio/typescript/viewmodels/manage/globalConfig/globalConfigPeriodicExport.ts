import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicExportSetupCommand = require("commands/database/documents/getPeriodicExportSetupCommand");
import savePeriodicExportSetupCommand = require("commands/maintenance/savePeriodicExportSetupCommand");
import document = require("models/database/documents/document");
import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import periodicExportSetup = require("models/database/documents/periodicExportSetup");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getEffectiveSettingsCommand = require("commands/database/globalConfig/getEffectiveSettingsCommand");
import saveGlobalSettingsCommand = require("commands/database/globalConfig/saveGlobalSettingsCommand");
import globalConfig = require("viewmodels/manage/globalConfig/globalConfig");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class globalConfigPeriodicExport extends viewModelBase {

    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;
    settingsDocument = ko.observable<document>();
    activated = ko.observable<boolean>(false);

    backupSetup = ko.observable<periodicExportSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup());

        var deferred = $.Deferred();
        var db: database = null;

        if (db) {
            if (settingsAccessAuthorizer.isForbidden()) {
                deferred.resolve({ can: true });
            } else {
                $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db))
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
            }
        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);

        var self = this;
        this.dirtyFlag = new ko.DirtyFlag([this.backupSetup]);
        this.isSaveEnabled = ko.computed(() => {
            var isNotReadOnly = !settingsAccessAuthorizer.isReadOnly();
            var onDisk = self.backupSetup().onDiskExportEnabled();
            var remote = self.backupSetup().remoteUploadEnabled();
            var hasAnyOption = onDisk || remote;
            var isDirty = this.dirtyFlag().isDirty();
            return isNotReadOnly && hasAnyOption && isDirty;
        });
    }

    attached() {
        super.attached();
        this.bindPopover();
        this.bindHintWatchers();
    }

    bindPopover() {
        $("#onDiskHint").popover({
            html: true,
            container: "body",
            trigger: "hover",
            content: "Database name will be appended to path in target configuration. <br /><br />" +
            "For value: <code>C:\\exports\\</code> target path will be: <code>C:\\exports\\{databaseName}</code>"
        });
        $(".folderHint").popover({
            html: true,
            container: "body",
            trigger: "hover",
            content: "Folder name will be replaced with database name being exported in local configuration."
        });
    }

    fetchPeriodicExportSetup(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getPeriodicExportSetupCommand(db, true)
            .execute()
            .done((result: periodicExportSetupDto) => {
                this.backupSetup().fromDto(result);
                this.activated(true);
            })
            .always(() => deferred.resolve());
        return deferred;
    }

    fetchPeriodicExportAccountsSettings(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getEffectiveSettingsCommand(db)
            .execute()
            .done((doc: document) => {
                this.settingsDocument(doc);
                this.backupSetup().fromDatabaseSettingsDto(doc.toDto(true));
            })
            .always(() => deferred.resolve());
        return deferred;
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        var db: database = null;
        if (db) {
            if (deleteConfig) {
                new deleteDocumentCommand("Raven/Global/Backup/Periodic/Setup", null)
                    .execute();
                this.deleteSettings(db);
            } else {
                var saveTask = new savePeriodicExportSetupCommand(this.backupSetup(), db, true).execute();
                saveTask.done((resultArray) => {
                    var newEtag = resultArray[0].ETag;
                    this.backupSetup().setEtag(newEtag);
                    (<any>this.settingsDocument()).__metadata["@etag"] = newEtag;
                    this.dirtyFlag().reset(); // Resync Changes
                });
            }
        }
    }

    private bindHintWatchers() {
        this.backupSetup().remoteUploadEnabled.subscribe(() => this.bindPopover());
        this.backupSetup().type.subscribe(() => this.bindPopover());
    }

    private deleteSettings(db: database) {
        var settingsDocument: any = this.settingsDocument();
        settingsDocument["@metadata"] = this.settingsDocument().__metadata;
        settingsDocument["@metadata"]["@etag"] = (<any>this.settingsDocument()).__metadata["@etag"];
        var copyOfSettings = settingsDocument.toDto(true);

        delete copyOfSettings["Settings"]["Raven/AWSAccessKey"];
        delete copyOfSettings["Settings"]["Raven/AzureStorageAccount"];
        delete copyOfSettings["SecuredSettings"]["Raven/AWSSecretKey"];
        delete copyOfSettings["SecuredSettings"]["Raven/AzureStorageKey"];

        var doc = new document(copyOfSettings);

        this.backupSetup(new periodicExportSetup());
        this.bindHintWatchers();
        this.backupSetup().fromDatabaseSettingsDto(copyOfSettings);
        
        var saveTask = new saveGlobalSettingsCommand(db, doc).execute();
        saveTask.done((saveResult: databaseDocumentSaveDto) => {
            this.backupSetup().setEtag(saveResult.ETag);
            (<any>this.settingsDocument()).__metadata["@etag"] = saveResult.ETag;
            this.dirtyFlag().reset(); //Resync Changes
        });
    }

    activateConfig() {
        this.activated(true);
        this.bindPopover();
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for periodic export?", "Are you sure?")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }
}

export = globalConfigPeriodicExport; 
