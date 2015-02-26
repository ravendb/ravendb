import viewModelBase = require("viewmodels/viewModelBase");
import versioningEntry = require("models/versioningEntry");
import appUrl = require("common/appUrl");
import getVersioningsCommand = require("commands/getVersioningsCommand");
import saveVersioningCommand = require("commands/saveVersioningCommand");

class globalConfigVersioning extends viewModelBase {
    activated = ko.observable<boolean>(false);

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
        var db = appUrl.getSystemDatabase();
        this.fetchVersioningEntries(db)
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(db) }));
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");
        this.toRemove = [];

        this.dirtyFlag = new ko.DirtyFlag([this.versionings]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioningEntries(db): JQueryPromise<any> {
        var task: JQueryPromise<versioningEntry[]> = new getVersioningsCommand(db, true).execute();

        task.done((versionings: versioningEntry[]) => this.versioningsLoaded(versionings));

        return task;
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        var db = appUrl.getSystemDatabase();
        if (deleteConfig) {
            var deleteTask = new saveVersioningCommand(
                db,
                [],
                this.versionings().map((v) => { return v.toDto(true); }).concat(this.toRemove.map((v) => { return v.toDto(true); })),
                true).execute();
            deleteTask.done((saveResult: bulkDocumentDto[]) => {
                this.activated(false);
                this.versionings([]);
                this.versioningsSaved(saveResult);
            });
        } else {
            var saveTask = new saveVersioningCommand(
                db,
                this.versionings().map((v) => { return v.toDto(true); }),
                this.toRemove.map((v) => { return v.toDto(true); }),
                true).execute();
            saveTask.done((saveResult: bulkDocumentDto[]) => {
                this.versioningsSaved(saveResult);
            });
        }
    }

    createNewVersioning() {
        this.versionings.push(new versioningEntry());
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
        this.activated(data.length > 0);
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

    activateConfig() {
        this.activated(true);
        this.versionings.push(this.defaultVersioningEntry());
    }

    private defaultVersioningEntry() {
        var entry = new versioningEntry({
            Id: "DefaultConfiguration",
            MaxRevisions: 5,
            Exclude: false
        });
        entry.fromDatabase(true);

        return entry;
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for versioning?", "Are you sure?")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }
}

export = globalConfigVersioning;