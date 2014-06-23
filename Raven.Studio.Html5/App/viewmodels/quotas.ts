import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import documentModel = require("models/document");
import database = require("models/database");
import appUrl = require("common/appUrl");

class quotas extends viewModelBase {
    settingsDocument = ko.observable<documentModel>();

    maximumSize = ko.observable<string>();
    warningLimitThreshold = ko.observable<string>();
    maxNumberOfDocs = ko.observable<string>();
    warningThresholdForDocs = ko.observable<string>();
 
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
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

        this.isSaveEnabled = ko.computed(() => viewModelBase.dirtyFlag().isDirty());
    }

    private fetchQuotas(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: documentModel) => {
                this.settingsDocument(document);
                this.maximumSize((document["Settings"]["Raven/Quotas/Size/HardLimitInKB"] / 1024).toString());
                this.warningLimitThreshold((document["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] / 1024).toString());
                this.maxNumberOfDocs(document["Settings"]["Raven/Quotas/Documents/HardLimit"]);
                this.warningThresholdForDocs(document["Settings"]["Raven/Quotas/Documents/SoftLimit"]);
            });
    }

    initializeDirtyFlag() {
        viewModelBase.dirtyFlag = new ko.DirtyFlag([
            this.maximumSize,
            this.warningLimitThreshold,
            this.maxNumberOfDocs,
            this.warningThresholdForDocs
        ]);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var document = new documentModel(this.settingsDocument().toDto(true));
            document["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = (parseInt(this.maximumSize()) * 1024).toString();
            document["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = (parseInt(this.warningLimitThreshold()) * 1024).toString();
            document["Settings"]["Raven/Quotas/Documents/HardLimit"] = this.maxNumberOfDocs().toString();
            document["Settings"]["Raven/Quotas/Documents/SoftLimit"] = this.warningThresholdForDocs().toString();
            var saveTask = new saveDatabaseSettingsCommand(db, document).execute();
            saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
                this.settingsDocument().__metadata['@etag'] = idAndEtag.ETag;
                viewModelBase.dirtyFlag().reset();
            });
        }
    }
}

export = quotas;
