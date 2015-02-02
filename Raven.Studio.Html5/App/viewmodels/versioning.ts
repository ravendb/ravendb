import viewModelBase = require("viewmodels/viewModelBase");
import document = require("models/document");
import collection = require("models/collection");
import database = require("models/database");
import versioningEntry = require("models/versioningEntry");
import appUrl = require("common/appUrl");
import getVersioningsCommand = require("commands/getVersioningsCommand");
import saveVersioningCommand = require("commands/saveVersioningCommand");

class versioning extends viewModelBase {
    versionings = ko.observableArray<versioningEntry>().extend({ required: true });
    toRemove: versioningEntry[];
    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.versionings = ko.observableArray<versioningEntry>();
    }

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchVersioningEntries(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('1UZ5WL');
        this.toRemove = [];

        this.dirtyFlag = new ko.DirtyFlag([this.versionings]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioningEntries(db): JQueryPromise<any>{
        var task: JQueryPromise<versioningEntry[]> = new getVersioningsCommand(db).execute();

        task.done((versionings: versioningEntry[]) => this.versioningsLoaded(versionings));

        return task;
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var saveTask = new saveVersioningCommand(
                db,
                this.versionings().map((v) => { return v.toDto(true); }),
                this.toRemove.map((v) => { return v.toDto(true); })
                ).execute();
            saveTask.done((saveResult: bulkDocumentDto[]) => {
                this.versioningsSaved(saveResult);
            });
        }
    }

    createNewVersioning() {
        this.versionings.push( new versioningEntry() ); 
    }

    removeVersioning(entry: versioningEntry) {
        if (entry.fromDatabase()) {
            // If this entry is in database schedule the removal
            this.toRemove.push(entry);
        }
        this.versionings.remove(entry);
    }

    versioningsLoaded(data: versioningEntry[]) {
        this.versionings(data);
        this.dirtyFlag().reset();
    }

    versioningsSaved(saveResult: bulkDocumentDto[]) {
        for (var i = 0; i < this.versionings().length; i++) {
            this.versionings()[i].__metadata.etag = saveResult[i].Etag;
        }

        // After save the collection names are not editable
        this.versionings().forEach((v: versioningEntry) => {
            v.fromDatabase(true);
        });

        this.dirtyFlag().reset();
        this.toRemove = [];
    }
}

export = versioning;
