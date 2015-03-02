import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import filesystem = require("models/filesystem/filesystem");
import renameFileCommand = require("commands/filesystem/renameFileCommand");


class fileRenameDialog extends dialogViewModelBase {

    private newName = ko.observable<string>();

    private nextTask = $.Deferred<string>();
    nextTaskStarted = false;

    constructor(private oldFilename: string, private fs: filesystem) {
        super();
        this.newName(oldFilename);
    }

    cancel() {
        dialog.close(this);
    }

    rename() {
        this.nextTaskStarted = true;
        new renameFileCommand(this.fs, this.oldFilename, this.newName()).execute()
            .done(() => this.nextTask.resolve(this.newName()))
            .fail(() => this.nextTask.reject());
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.nextTaskStarted) {
            this.nextTask.reject();
        }
    }

    onExit() {
        return this.nextTask.promise();
    }
}

export = fileRenameDialog;