import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import counterChange = require("models/counter/counterChange");
import updateCounterCommand = require("commands/counter/updateCounterCommand");

class editCounterDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    isNew = ko.observable(false);
    editedCounter = ko.observable<counterChange>();
    private maxNameLength = 200;

    constructor(editedCounter?: counterChange) {
        super();

        if (!editedCounter) {
            this.isNew(true);
            this.editedCounter(counterChange.empty());
        } else {
            this.editedCounter(editedCounter);
        }
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedCounter(), this.isNew());
        dialog.close(this);
    }
    
    attached() {
        super.attached();

        var inputElementGroupName: any = $("#group")[0];
        this.editedCounter().group.subscribe((newCounterId) => {
            var errorMessage = this.checkName(newCounterId, "group name");
            inputElementGroupName.setCustomValidity(errorMessage);
        });

        var inputElementCounterName: any = $("#counterName")[0];
        this.editedCounter().counterName.subscribe((newCounterId) => {
            var errorMessage = this.checkName(newCounterId, "counter name");
            inputElementCounterName.setCustomValidity(errorMessage);
        });

        //todo: maybe check validity of delta
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.updateTaskStarted) {
            this.updateTask.reject();
        }
    }

    private checkName(name: string, fieldName): string {
        var message = "";
        if (!$.trim(name)) {
            message = "An empty " + fieldName + " is forbidden for use!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The  " + fieldName + " length can't exceed " + this.maxNameLength + " characters!";
        }
        return message;
    }
}

export = editCounterDialog;