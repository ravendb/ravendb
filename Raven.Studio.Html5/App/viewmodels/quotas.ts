import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import document = require("models/document");
import database = require("models/database");
import appUrl = require("common/appUrl");
import shell = require('viewmodels/shell');

class quotas extends viewModelBase {
    settingsDocument = ko.observable<document>();
    maximumSize = ko.observable<number>();
    warningLimitThreshold = ko.observable<number>();
    maxNumberOfDocs = ko.observable<number>();
    warningThresholdForDocs = ko.observable<number>();
    isSaveEnabled: KnockoutComputed<boolean>;
    isForbidden = ko.observable<boolean>(false);

    constructor() {
        super();
    }

    canActivate(args: any): any {
        super.canActivate(args);
        var deferred = $.Deferred();

        this.isForbidden(shell.isGlobalAdmin() == false);
        if (this.isForbidden() == false) {
            var db = this.activeDatabase();
            // fetch current quotas from the database
            this.fetchQuotas(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        } else {
            deferred.resolve({ can: true });
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('594W7T');
        this.initializeDirtyFlag();

        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty() === true);
    }

    private fetchQuotas(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((doc: document) => {
                this.settingsDocument(doc);
                this.maximumSize(doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"] / 1024);
                this.warningLimitThreshold(doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] / 1024);
                this.maxNumberOfDocs(doc["Settings"]["Raven/Quotas/Documents/HardLimit"]);
                this.warningThresholdForDocs(doc["Settings"]["Raven/Quotas/Documents/SoftLimit"]);
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
            var settingsDocument = this.settingsDocument();
            settingsDocument['@metadata'] = this.settingsDocument().__metadata;
            settingsDocument['@metadata']['@etag'] = this.settingsDocument().__metadata['@etag'];
            var doc = new document(settingsDocument.toDto(true));
            doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = this.maximumSize() * 1024;
            doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = this.warningLimitThreshold() * 1024;
            doc["Settings"]["Raven/Quotas/Documents/HardLimit"] = this.maxNumberOfDocs();
            doc["Settings"]["Raven/Quotas/Documents/SoftLimit"] = this.warningThresholdForDocs();
            var saveTask = new saveDatabaseSettingsCommand(db, doc).execute();
            saveTask.done((saveResult: databaseDocumentSaveDto) => {
                this.settingsDocument().__metadata['@etag'] = saveResult.ETag;
                this.dirtyFlag().reset(); //Resync Changes
            });
        }
    }
}

export = quotas;
