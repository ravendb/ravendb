import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import typeChange = require("models/timeSeries/typeChange");

class editTypeDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    editedType = ko.observable<typeChange>();
    isNew: KnockoutComputed<boolean>;

    constructor(editedType: typeChange, isNew: boolean) {
        super();
        this.editedType(editedType);
        this.isNew = ko.computed(() => isNew && this.editedType().isNew());
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedType());
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

export = editTypeDialog;