import dialog = require("plugins/dialog");
import deleteCountersByGroupCommand = require("commands/counter/deleteCountersByGroupCommand");
import counterGroup = require("models/counter/counterGroup");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import counterStorage = require("models/counter/counterStorage");

class deleteGroup extends dialogViewModelBase {

    public deletionTask = $.Deferred();
    private deletionStarted = false;

    constructor(private group: counterGroup, private cs: counterStorage) {
        super();
    }

    deleteGroup() {
        var deleteCommand = new deleteCountersByGroupCommand(this.group, this.cs);
        var deleteCommandTask = deleteCommand.execute();
        deleteCommandTask.done((result) => this.deletionTask.resolve(result));
        deleteCommandTask.fail(response => this.deletionTask.reject(response));
        this.deletionStarted = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteGroup;