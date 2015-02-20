import viewModelBase = require("viewmodels/viewModelBase");
import getEffectiveSettingsCommand = require("commands/getEffectiveSettingsCommand");
import saveGlobalSettingsCommand = require("commands/saveGlobalSettingsCommand");
import document = require("models/document");
import database = require("models/database");
import appUrl = require("common/appUrl");

class globalConfigQuotas extends viewModelBase {
    settingsDocument = ko.observable<document>();

    activated = ko.observable<boolean>(false);

    maximumSize = ko.observable<number>();
    warningLimitThreshold = ko.observable<number>();
    maxNumberOfDocs = ko.observable<number>();
    warningThresholdForDocs = ko.observable<number>();
 
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();
        if (db) {
            // fetch current quotas from the database
            this.fetchQuotas(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.initializeDirtyFlag();
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty() === true);
    }

    private fetchQuotas(db: database): JQueryPromise<any> {
        return new getEffectiveSettingsCommand(db)
            .execute()
            .done((doc: document) => {
                this.settingsDocument(doc);
                // we make decision based on first available property 
                var activated = !!doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"];
                this.activated(activated);
                if (activated) {
                    this.maximumSize(doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"] / 1024);
                    this.warningLimitThreshold(doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] / 1024);
                    this.maxNumberOfDocs(doc["Settings"]["Raven/Quotas/Documents/HardLimit"]);
                    this.warningThresholdForDocs(doc["Settings"]["Raven/Quotas/Documents/SoftLimit"]);
                }
            });
    }

    initializeDirtyFlag() {
        this.dirtyFlag = new ko.DirtyFlag([
            this.maximumSize,
            this.warningLimitThreshold,
            this.maxNumberOfDocs,
            this.warningThresholdForDocs
        ]);
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig:boolean) {
        var db = appUrl.getSystemDatabase();
        if (db) {
            var settingsDocument = this.settingsDocument();
            settingsDocument["@metadata"] = this.settingsDocument().__metadata;
            settingsDocument["@metadata"]["@etag"] = this.settingsDocument().__metadata["@etag"];
            var doc = new document(settingsDocument.toDto(true));

            if (deleteConfig) {
                delete doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"];
                delete doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"];
                delete doc["Settings"]["Raven/Quotas/Documents/HardLimit"];
                delete doc["Settings"]["Raven/Quotas/Documents/SoftLimit"];

                this.maximumSize(null);
                this.warningLimitThreshold(null);
                this.maxNumberOfDocs(null);
                this.warningThresholdForDocs(null);
            } else {
                doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = this.maximumSize() * 1024;
                doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = this.warningLimitThreshold() * 1024;
                doc["Settings"]["Raven/Quotas/Documents/HardLimit"] = this.maxNumberOfDocs();
                doc["Settings"]["Raven/Quotas/Documents/SoftLimit"] = this.warningThresholdForDocs();
            }

            var saveTask = new saveGlobalSettingsCommand(db, doc).execute();
            saveTask.done((saveResult: databaseDocumentSaveDto) => {
                this.settingsDocument().__metadata["@etag"] = saveResult.ETag;
                this.dirtyFlag().reset(); //Resync Changes
            });
        }
    }

    activateConfig() {
        this.activated(true);
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for quotas?", "Please note that databases with quotas bundle enabled and without local configuration may crash.")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }
}

export = globalConfigQuotas;
