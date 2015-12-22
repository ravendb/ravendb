import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import counterChange = require("models/counter/counterChange");

class editCounterDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    editedCounter = ko.observable<counterChange>();
    isNew: KnockoutComputed<boolean>;

    constructor(editedCounter?: counterChange) {
        super();
        this.editedCounter(!editedCounter ? counterChange.empty() : editedCounter);
        this.isNew = ko.computed(() => !!this.editedCounter() && this.editedCounter().isNew());
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedCounter());
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.updateTaskStarted) {
            this.updateTask.reject();
        }
    }
}

export = editCounterDialog;
