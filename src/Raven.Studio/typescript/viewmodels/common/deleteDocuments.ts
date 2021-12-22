import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");

class deleteDocuments extends dialogViewModelBase {

    view = require("views/common/deleteDocuments.html");

    private documentIds = ko.observableArray<string>();
    private deletionStarted = false;
    deletionTask = $.Deferred<void>();

    constructor(documentIds: Array<string>, private db: database) {
        super(null);

        if (documentIds.length === 0) {
            throw new Error("Must have at least one document id to delete.");
        }

        this.documentIds(documentIds);
    }

    deleteDocs() {
        const docCount = this.documentIds().length;
        const docsDescription = docCount === 1 ? this.documentIds()[0] : docCount + " docs";

        new deleteDocumentsCommand(this.documentIds(), this.db)
            .execute()
            .done(() => {
                messagePublisher.reportSuccess("Deleted " + docsDescription);
                this.deletionTask.resolve();
            })
            .fail(response => {
                messagePublisher.reportError("Failed to delete " + docsDescription,
                    response.responseText,
                    response.statusText);
                this.deletionTask.reject(response);
            });

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
