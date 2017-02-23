import dialog = require("plugins/dialog");
import deleteCollectionCommand = require("commands/database/documents/deleteCollectionCommand");
import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCollection extends dialogViewModelBase {

    operationIdTask = $.Deferred<operationIdDto>();
    private deletionStarted = false;
    isAllDocuments: boolean;

    constructor(private collectionName: string, private db: database, private itemsToDelete: number, private excludedIds: Array<string>) {
        super();
        this.isAllDocuments = collection.allDocumentsCollectionName === collectionName;
    }

    deleteCollection() {
        const collectionName = this.isAllDocuments ? "*" : this.collectionName;

        new deleteCollectionCommand(collectionName, this.db, this.excludedIds)
            .execute()
            .done((result) => this.operationIdTask.resolve(result))
            .fail(response => this.operationIdTask.reject(response));

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
            this.operationIdTask.reject();
        }
    }
}

export = deleteCollection;
