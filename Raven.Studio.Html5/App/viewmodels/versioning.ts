import viewModelBase = require("viewmodels/viewModelBase");
import versioningEntry = require("models/versioningEntry");
import appUrl = require("common/appUrl");
import saveVersioningCommand = require("commands/saveVersioningCommand");
import getEffectiveVersioningsCommand = require("commands/getEffectiveVersioningsCommand");
import configurationDocument = require("models/configurationDocument");

class versioning extends viewModelBase {
    versionings = ko.observableArray<configurationDocument<versioningEntry>>().extend({ required: true });
    toRemove: configurationDocument<versioningEntry>[];
    isSaveEnabled: KnockoutComputed<boolean>;

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    constructor() {
        super();
        this.versionings = ko.observableArray<configurationDocument<versioningEntry>>();
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
        this.updateHelpLink("1UZ5WL");
        this.toRemove = [];

        this.dirtyFlag = new ko.DirtyFlag([this.versionings, this.usingGlobal]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioningEntries(db): JQueryPromise<any>{
        var task: JQueryPromise<configurationDocument<versioningEntry>[]> = new getEffectiveVersioningsCommand(db).execute();
        task.done((versionings: configurationDocument<versioningEntry>[]) => this.versioningsLoaded(versionings));
        return task;
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var saveTask = new saveVersioningCommand(
                db,
                this.versionings().filter(v => v.localExists()).map((v) => { return v.mergedDocument().toDto(true); }),
                this.toRemove.map((v) => { return v.mergedDocument().toDto(true); })
                ).execute();
            saveTask.done((saveResult: bulkDocumentDto[]) => {
                this.versioningsSaved(saveResult);
            });
        }
    }

    createNewVersioning() {
        this.versionings.push(new configurationDocument({
            GlobalExists: false,
            GlobalDocument: null,
            LocalExists: true,
            MergedDocument: new versioningEntry(),
            Etag: null,
            Metadata: null
        }));
    }

    removeVersioning(entry: configurationDocument<versioningEntry>) {
        if (entry.mergedDocument().fromDatabase()) {
            // If this entry is in database schedule the removal
            this.toRemove.push(entry);
        }
        this.versionings.remove(entry);
    }

    versioningsLoaded(data: configurationDocument<versioningEntry>[]) {
        this.versionings(data);
        this.hasGlobalValues(!!data.first(config => config.globalExists()));

        this.usingGlobal(this.hasGlobalValues() && !data.first(config => config.localExists()));
        this.dirtyFlag().reset();
    }

    versioningsSaved(saveResult: bulkDocumentDto[]) {
        var locals = this.versionings().filter(v => v.localExists());
        for (var i = 0; i < locals.length; i++) {
            locals[i].mergedDocument().__metadata.etag = saveResult[i].Etag;
        }

        // After save the collection names are not editable
        this.versionings().forEach((c :configurationDocument<versioningEntry>) => {
            c.mergedDocument().fromDatabase(true);
        });

        this.dirtyFlag().reset();
        this.toRemove = [];
    }

    override(value: boolean, config: configurationDocument<versioningEntry>) {
        config.localExists(value);
        if (!config.localExists()) {
            this.toRemove.push(config);
            config.copyFromGlobal();
        } else {
            this.toRemove.remove(config);
        }
    }

    useLocal() {
        this.usingGlobal(false);
    }

    useGlobal() {
        this.usingGlobal(true);
        this.toRemove.pushAll(this.versionings().filter(c => c.localExists()));
        var newVersionsings = this.versionings().filter(c => c.globalExists());
        newVersionsings.forEach(v => v.copyFromGlobal());

        this.versionings(newVersionsings);
    }
}

export = versioning;
