import document = require("models/document");
import dialog = require("plugins/dialog");
import deleteFileCommand = require("commands/filesystem/deleteFileCommand");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteFiles extends dialogViewModelBase {

    private files = ko.observableArray<documentBase>();
    private deletionStarted = false;
    public deletionTask = $.Deferred(); // Gives consumers a way to know when the async delete operation completes.

    constructor(files: Array<documentBase>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (files.length === 0) {
            throw new Error("Must have at least one file to delete.");
        }

        this.files(files);
    }

    deleteFiles() {
        var deletedFilesIds = this.files().map(i => i.getId());
        var deletionTasks = [];
        for (var i = 0; i < deletedFilesIds.length; i++) {
            var deleteCommand = new deleteFileCommand(appUrl.getFilesystem(), deletedFilesIds[i]);
            deletionTasks.push(deleteCommand.execute());
        }

        var combinedTask = $.when(deletionTasks);

        combinedTask.done(() => this.deletionTask.resolve(this.files()));
        combinedTask.fail(response => this.deletionTask.reject(response));

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

export = deleteFiles; 