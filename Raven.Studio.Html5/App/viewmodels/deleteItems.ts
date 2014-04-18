import document = require("models/document");
import file = require("models/filesystem/file");
import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/deleteDocumentsCommand");
import deleteFilesCommand = require("commands/filesystem/deleteFilesCommand");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteItems extends dialogViewModelBase {

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

    deleteItems() {
        var deleteItemsIds = this.documents().map(i => i.getId());
        var deleteCommand;
        if (this.documents()[0] instanceof document) {
            deleteCommand = new deleteDocumentsCommand(deleteItemsIds, appUrl.getDatabase());
        }
        else if (this.documents()[0] instanceof file) {
            deleteCommand = new deleteFilesCommand(deleteItemsIds, appUrl.getFilesystem());
        }
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

export = deleteItems;