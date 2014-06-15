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
    //versionings: KnockoutObservableArray<versioningEntry>;
    numOfVersionings = ko.computed(() => this.versionings().length);
    toRemove: versioningEntry[];
    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.versionings = ko.observableArray<versioningEntry>();
    }

    activate(args) {
        super.activate(args);
        this.fetchVersioningEntries();

        this.toRemove = [];

        this.numOfVersionings = ko.computed(() => this.versionings().length);

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.versionings]);
        this.isSaveEnabled = ko.computed<boolean>(() => viewModelBase.dirtyFlag().isDirty());
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

    fetchVersioningEntries() {
        var task: JQueryPromise<versioningEntry[]> = new getVersioningsCommand(this.activeDatabase()).execute();
        task.done((versionings: versioningEntry[]) => {
            this.versioningsLoaded(versionings);
        });
    }

    versioningsLoaded(data: versioningEntry[]) {
        this.versionings(data);
        viewModelBase.dirtyFlag().reset();
    }

    versioningsSaved(saveResult: bulkDocumentDto[]) {
        for (var i = 0; i < this.versionings().length; i++) {
            this.versionings()[i].__metadata.etag = saveResult[i].Etag;
        }

        // After save the collection names are not editable
        this.versionings().forEach((v: versioningEntry) => {
            v.fromDatabase(true);
        });

        viewModelBase.dirtyFlag().reset();
        this.toRemove = [];
    }
}

export = versioning;
