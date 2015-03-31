import document = require("models/document");
import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/deleteDocumentsCommand");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteDocuments extends dialogViewModelBase {

    private documents = ko.observableArray<documentBase>();
    private deletionStarted = false;
    public deletionTask = $.Deferred(); // Gives consumers a way to know when the async delete operation completes.

    constructor(documents: Array<documentBase>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (documents.length === 0) {
            throw new Error("Must have at least one document to delete.");
        }

        this.documents(documents);
    }

    deleteDocs() {
        var deletedDocIds = this.documents().map(i => i.getId());
        var deleteCommand = new deleteDocumentsCommand(deletedDocIds, appUrl.getDatabase());
        var deleteCommandTask = deleteCommand.execute();

        deleteCommandTask.done(() => this.deletionTask.resolve(this.documents()));
        deleteCommandTask.fail(response => this.deletionTask.reject(response));

        this.deletionStarted = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args) {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteDocuments;