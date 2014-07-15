import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import documentModel = require("models/document");
import database = require("models/database");
import appUrl = require("common/appUrl");

class quotas extends viewModelBase {
    settingsDocument = ko.observable<documentModel>();

    maximumSize = ko.observable<number>();
    warningLimitThreshold = ko.observable<number>();
    maxNumberOfDocs = ko.observable<number>();
    warningThresholdForDocs = ko.observable<number>();
 
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

        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    private fetchQuotas(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: documentModel) => {
                this.settingsDocument(document);
                this.maximumSize(document["Settings"]["Raven/Quotas/Size/HardLimitInKB"] / 1024);
                this.warningLimitThreshold(document["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] / 1024);
                this.maxNumberOfDocs(document["Settings"]["Raven/Quotas/Documents/HardLimit"]);
                this.warningThresholdForDocs(document["Settings"]["Raven/Quotas/Documents/SoftLimit"]);
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
        var db = this.activeDatabase();
        if (db) {
            var document = new documentModel(this.settingsDocument().toDto(true));
            document["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = this.maximumSize() * 1024;
            document["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = this.warningLimitThreshold() * 1024;
            document["Settings"]["Raven/Quotas/Documents/HardLimit"] = this.maxNumberOfDocs();
            document["Settings"]["Raven/Quotas/Documents/SoftLimit"] = this.warningThresholdForDocs();
            var saveTask = new saveDatabaseSettingsCommand(db, document).execute();
            saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
                this.settingsDocument().__metadata['@etag'] = idAndEtag.ETag;
                this.dirtyFlag().reset();
            });
        }
    }
}

export = quotas;
