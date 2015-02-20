import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicExportSetupCommand = require("commands/getPeriodicExportSetupCommand");
import savePeriodicExportSetupCommand = require("commands/savePeriodicExportSetupCommand");
import document = require("models/document");
import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import periodicExportSetup = require("models/periodicExportSetup");
import appUrl = require("common/appUrl");
import database = require("models/database");
import getEffectiveSettingsCommand = require("commands/getEffectiveSettingsCommand");
import saveGlobalSettingsCommand = require("commands/saveGlobalSettingsCommand");

class globalConfigPeriodicExport extends viewModelBase {

    settingsDocument = ko.observable<document>();
    activated = ko.observable<boolean>(false);

    backupSetup = ko.observable<periodicExportSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);
        this.backupSetup(new periodicExportSetup);

        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();

        if (db) {
            $.when(this.fetchPeriodicExportSetup(db), this.fetchPeriodicExportAccountsSettings(db))
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);

        var self = this;
        this.dirtyFlag = new ko.DirtyFlag([this.backupSetup]);
        this.isSaveEnabled = ko.computed(() => {
            var onDisk = self.backupSetup().onDiskExportEnabled();
            var remote = self.backupSetup().remoteUploadEnabled();
            var hasAnyOption = onDisk || remote;
            var isDirty = this.dirtyFlag().isDirty();
            return hasAnyOption && isDirty;
        });
    }

    attached() {
        this.bindPopover();
    }

    bindPopover() {
        $("#onDiskHint").popover({
            html: true,
            container: $("body"),
            trigger: "hover",
            content: "Database name will be appended to path in target configuration. <br /><br />" +
            "For value: <code>C:\\exports\\</code> target path will be: <code>C:\\exports\\{databaseName}</code>"
        });
    }

    fetchPeriodicExportSetup(db): JQueryPromise<any> {
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

    fetchPeriodicExportAccountsSettings(db): JQueryPromise<any> {
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
        var db = appUrl.getSystemDatabase();
        if (db) {
            if (deleteConfig) {
                new deleteDocumentCommand("Raven/Global/Backup/Periodic/Setup", appUrl.getSystemDatabase())
                    .execute();
                this.deleteSettings(db);
            } else {
                var saveTask = new savePeriodicExportSetupCommand(this.backupSetup(), db, true).execute();
                saveTask.done((resultArray) => {
                    var newEtag = resultArray[0].ETag;
                    this.backupSetup().setEtag(newEtag);
                    this.settingsDocument().__metadata["@etag"] = newEtag;
                    this.dirtyFlag().reset(); // Resync Changes
                });
            }
        }
    }

    private deleteSettings(db: database) {
        var settingsDocument = this.settingsDocument();
        settingsDocument["@metadata"] = this.settingsDocument().__metadata;
        settingsDocument["@metadata"]["@etag"] = this.settingsDocument().__metadata["@etag"];
        var copyOfSettings = settingsDocument.toDto(true);

        delete copyOfSettings["Settings"]["Raven/AWSAccessKey"];
        delete copyOfSettings["Settings"]["Raven/AzureStorageAccount"];
        delete copyOfSettings["SecuredSettings"]["Raven/AWSSecretKey"];
        delete copyOfSettings["SecuredSettings"]["Raven/AzureStorageKey"];

        var doc = new document(copyOfSettings);

        this.backupSetup(new periodicExportSetup());
        this.backupSetup().fromDatabaseSettingsDto(copyOfSettings);
        
        var saveTask = new saveGlobalSettingsCommand(db, doc).execute();
        saveTask.done((saveResult: databaseDocumentSaveDto) => {
            this.backupSetup().setEtag(saveResult.ETag);
            this.settingsDocument().__metadata["@etag"] = saveResult.ETag;
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