import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class savePatch extends dialogViewModelBase {

    private nextTask = $.Deferred<string>();
    nextTaskStarted = false;
    patchName = ko.observable<string>();

    cancel() {
        dialog.close(this);
    }

    saveThePatch() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.patchName());
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
}

export = savePatch;