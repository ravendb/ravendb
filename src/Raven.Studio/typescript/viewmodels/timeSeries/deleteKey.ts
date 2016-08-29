import dialog = require("plugins/dialog");
import deleteKeyCommand = require("commands/timeSeries/deleteKeyCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import timeSeries = require("models/timeSeries/timeSeries");

class deleteKey extends dialogViewModelBase {

    deletionTask = $.Deferred();
    private deletionStarted = false;

    constructor(private type: string, private key: string, private ts: timeSeries) {
        super();
    }

    deleteKey() {
        new deleteKeyCommand(this.type, this.key, this.ts)
            .execute()
            .done((result) => this.deletionTask.resolve(result))
            .fail(response => this.deletionTask.reject(response));
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

export = deleteKey;
