import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import filesystem = require("models/filesystem/filesystem");
import deleteFilesMatchingQueryCommand = require("commands/filesystem/deleteFilesMatchingQueryCommand");

class deleteFilesMatchingQueryConfirm extends dialogViewModelBase {
    constructor(private queryText: string, private totalFilesCount: number, private fs: filesystem) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deleteFiles() {
        new deleteFilesMatchingQueryCommand(this.queryText, this.fs).execute();
        dialog.close(this);
    }
}

export = deleteFilesMatchingQueryConfirm;