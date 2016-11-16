import viewModelBase = require("viewmodels/viewModelBase");
import getEffectiveSettingsCommand = require("commands/database/globalConfig/getEffectiveSettingsCommand");
import saveGlobalSettingsCommand = require("commands/database/globalConfig/saveGlobalSettingsCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import globalConfig = require("viewmodels/manage/globalConfig/globalConfig");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import eventsCollector = require("common/eventsCollector");

class globalConfigQuotas extends viewModelBase {

    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;
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
        if (settingsAccessAuthorizer.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            // fetch current quotas from the database
            this.fetchQuotas(null)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.initializeDirtyFlag();
        this.isSaveEnabled = ko.computed(() => !settingsAccessAuthorizer.isReadOnly() && this.dirtyFlag().isDirty());
    }

    private fetchQuotas(db: database): JQueryPromise<any> {
        return new getEffectiveSettingsCommand(db)
            .execute()
            .done((doc: document) => {
                this.settingsDocument(doc);
                // we make decision based on first available property 
                var activated = !!(<any>doc)["Settings"]["Raven/Quotas/Size/HardLimitInKB"];
                this.activated(activated);
                if (activated) {
                    this.maximumSize((<any>doc)["Settings"]["Raven/Quotas/Size/HardLimitInKB"] / 1024);
                    this.warningLimitThreshold((<any>doc)["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] / 1024);
                    this.maxNumberOfDocs((<any>doc)["Settings"]["Raven/Quotas/Documents/HardLimit"]);
                    this.warningThresholdForDocs((<any>doc)["Settings"]["Raven/Quotas/Documents/SoftLimit"]);
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
        eventsCollector.default.reportEvent("global-config-quotas", "save");
        this.syncChanges(false);
    }

    syncChanges(deleteConfig:boolean) {
        var settingsDocument:any = this.settingsDocument();
        settingsDocument["@metadata"] = this.settingsDocument().__metadata;
        settingsDocument["@metadata"]["@etag"] = (<any>this.settingsDocument()).__metadata["@etag"];
        var doc: any = new document(settingsDocument.toDto(true));

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

        var saveTask = new saveGlobalSettingsCommand(null, doc).execute();
        saveTask.done((saveResult: databaseDocumentSaveDto) => {
            (<any>this.settingsDocument()).__metadata["@etag"] = saveResult.ETag;
            this.dirtyFlag().reset(); //Resync Changes
        });
    }

    activateConfig() {
        eventsCollector.default.reportEvent("global-config-quotas", "activate");
        this.activated(true);
    }

    disactivateConfig() {
        eventsCollector.default.reportEvent("global-config-quotas", "disactivate");
        this.confirmationMessage("Delete global configuration for quotas?", "Please note that databases with quotas bundle enabled and without local configuration may crash.")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }
}

export = globalConfigQuotas;
