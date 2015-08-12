import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import pointChange = require("models/timeSeries/pointChange");

class editPointDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    editedPoint = ko.observable<pointChange>();
    isNew: KnockoutComputed<boolean>;

    constructor(editedPoint: pointChange, isNew: boolean) {
        super();
        this.editedPoint(editedPoint);
        this.isNew = ko.computed(() => isNew && this.editedPoint().isNew());
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedPoint());
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

export = editPointDialog;