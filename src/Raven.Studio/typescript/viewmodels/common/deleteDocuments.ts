import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");

class deleteDocuments extends dialogViewModelBase {

    private documents = ko.observableArray<documentBase>();
    private deletionStarted = false;
    deletionTask = $.Deferred<void>();

    constructor(documents: Array<documentBase>, private db: database) {
        super(null);

        if (documents.length === 0) {
            throw new Error("Must have at least one document to delete.");
        }

        this.documents(documents);
    }

    deleteDocs() {
        const deletedDocIds = this.documents().map(i => i.getId());

        new deleteDocumentsCommand(deletedDocIds, this.db)
            .execute()
            .done(() => this.deletionTask.resolve())
            .fail(response => this.deletionTask.reject(response));

        this.deletionStarted = true;
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    deactivate(args: any) {
        super.deactivate(args);
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteDocuments;
