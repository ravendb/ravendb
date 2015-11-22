import resetIndexCommand = require("commands/filesystem/resetIndexCommand");
import dialog = require("plugins/dialog");
import filesystem = require("models/filesystem/filesystem");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class resetIndexConfirm extends dialogViewModelBase {

    resetTask = $.Deferred();

    constructor(private fs: filesystem) {
        super();
    }

    resetIndex() {
        new resetIndexCommand(this.fs).execute().done(() => this.resetTask.resolve()).fail(() => this.resetTask.reject());
        dialog.close(this);
    }

    cancel() {
        this.resetTask.reject();
        dialog.close(this);
    }
}

export = resetIndexConfirm;
