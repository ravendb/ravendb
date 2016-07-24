import dialog = require("plugins/dialog");
import deleteCollectionCommand = require("commands/database/documents/deleteCollectionCommand");
import collection = require("models/database/documents/collection");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCollection extends dialogViewModelBase {

    public deletionTask = $.Deferred<operationIdDto>();
    private deletionStarted = false;
    isAllDocuments: boolean;

    constructor(private collection: collection) {
        super();
        this.isAllDocuments = collection.isAllDocuments;
    }

    deleteCollection() {
        var collectionName = this.isAllDocuments ? "*" : this.collection.name;
        var deleteCommand = new deleteCollectionCommand(collectionName, appUrl.getDatabase());
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

export = deleteCollection;
