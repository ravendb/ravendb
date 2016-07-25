import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import filesystem = require("models/filesystem/filesystem");
import deleteFilesMatchingQueryCommand = require("commands/filesystem/deleteFilesMatchingQueryCommand");

class deleteFilesMatchingQueryConfirm extends dialogViewModelBase {
    private deletionStarted = false;
    deletionTask = $.Deferred(); // Gives consumers a way to know when the async delete operation completes.

    constructor(private queryText: string, private totalFilesCount: number, private fs: filesystem) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deleteFiles() {
        var deleteCommandTask = new deleteFilesMatchingQueryCommand(this.queryText, this.fs).execute();

        deleteCommandTask.done(() => this.deletionTask.resolve());
        deleteCommandTask.fail(response => this.deletionTask.reject(response));

        this.deletionStarted = true;
        dialog.close(this);
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

export = deleteFilesMatchingQueryConfirm;
