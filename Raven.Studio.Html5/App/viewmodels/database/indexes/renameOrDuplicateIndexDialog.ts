import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class renameOrDuplicateIndexDialog extends dialogViewModelBase {

    private renameTask = $.Deferred<void>();
    private saveAsNewTask = $.Deferred<void>();

    constructor(private originalName: string, private newName: string) {
        super();
    }

    rename() {
        this.renameTask.resolve();
        dialog.close(this);
    }

    createNew() {
        this.saveAsNewTask.resolve();
        dialog.close(this);
    }

    getRenameTask(): JQueryPromise<void> {
        return this.renameTask.promise();
    }

    getSaveAsNewTask(): JQueryPromise<void> {
        return this.saveAsNewTask.promise();
    }

    close() {
        this.renameTask.reject();
        this.saveAsNewTask.reject();
        dialog.close(this);
    }

}

export = renameOrDuplicateIndexDialog; 
