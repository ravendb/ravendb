import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import counter = require("models/counter/counter");

class editCounterDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    
    isNewCounter = ko.observable(false);
    editedCounter= ko.observable<counter>();
    counterDelta=ko.observable(0);
    
    private maxNameLength = 200;

    constructor(editedCounter?: counter) {
        super();

        if (!editedCounter) {
            this.isNewCounter(true);
            this.editedCounter(new counter({ Name: '', Group: '', OverallTotal:0,Servers:[]}));
        } else {
            this.editedCounter(editedCounter);
        }
        this.counterDelta(0);
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedCounter(), this.counterDelta());
        dialog.close(this);
    }
    
    attached() {
        super.attached();
        this.counterDelta(0);
        var inputElement: any = $("#counterId")[0];
        this.editedCounter().id.subscribe((newCounterId) => {
            var errorMessage: string = '';

            if ((errorMessage = this.CheckName(newCounterId, 'counter name')) != '') { }
            inputElement.setCustomValidity(errorMessage);
        });
        this.editedCounter().group.subscribe((newCounterId) => {
            var errorMessage: string = '';

            if ((errorMessage = this.CheckName(newCounterId, 'group name')) != '') { }
            inputElement.setCustomValidity(errorMessage);
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

    private CheckName(name: string, fieldName): string {
        var message = '';
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