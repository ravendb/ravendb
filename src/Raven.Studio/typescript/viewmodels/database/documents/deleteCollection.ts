import dialog = require("plugins/dialog");
import deleteCollectionCommand = require("commands/database/documents/deleteCollectionCommand");
import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

class deleteCollection extends dialogViewModelBase {

    operationIdTask = $.Deferred<operationIdDto>();
    private deletionStarted = false;
    isAllDocuments: boolean;
    hasHiloDocuments: boolean;

    constructor(private collectionName: string, private db: database, private itemsToDelete: number, private excludedIds: Array<string>) {
        super();
        this.isAllDocuments = collection.allDocumentsCollectionName === collectionName;
        this.hasHiloDocuments = collectionsTracker.default.getCollectionCount(collection.hiloCollectionName) > 0;
    }

    deleteCollection() {
        const collectionName = this.isAllDocuments ? "@all_docs" : this.collectionName;

        new deleteCollectionCommand(collectionName, this.db, this.excludedIds)
            .execute()
            .done((result) => this.operationIdTask.resolve(result))
            .fail(response => this.operationIdTask.reject(response));

        this.deletionStarted = true;
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
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
