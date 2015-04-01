import patchDocument = require("models/database/patch/patchDocument");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');

class loadPatch extends dialogViewModelBase {

    private nextTask = $.Deferred<patchDocument>();
    nextTaskStarted = false;
    patchName = ko.observable<string>("");
    patches = ko.observableArray<patchDocument>();
    patch = ko.observable<patchDocument>();

    constructor(private database: database) {
        super();
    }

    activate() {
        this.fetchAllPatches();
        this.patchName = ko.observable<string>("Select patch to load");
    }

    cancel() {
        dialog.close(this);
    }

    loadThePatch() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.patch());
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.nextTaskStarted) {
            this.nextTask.reject();
        }
    }

    onExit() {
        return this.nextTask.promise();
    }

    fetchAllPatches() {
        new getPatchesCommand(this.database)
            .execute()
            .done((patches: patchDocument[]) => this.patches(patches));
    }

    setSelectedPatch(patch: patchDocument) {
        this.patch(patch);
        this.patchName(patch.name());
    }
}

export = loadPatch;