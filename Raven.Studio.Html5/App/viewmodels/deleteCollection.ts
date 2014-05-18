import document = require("models/document");
import dialog = require("plugins/dialog");
import deleteCollectionCommand = require("commands/deleteCollectionCommand");
import collection = require("models/collection");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCollection extends dialogViewModelBase {

    public deletionTask = $.Deferred();
    private deletionStarted = false;

    constructor(private collection: collection) {
        super();
    }

    deleteCollection() {
        var deleteCommand = new deleteCollectionCommand(this.collection.name, appUrl.getDatabase());
        var deleteCommandTask = deleteCommand.execute();
        deleteCommandTask.done(() => this.deletionTask.resolve(this.collection));
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

export = deleteCollection;