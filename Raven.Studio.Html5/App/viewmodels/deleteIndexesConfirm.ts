import index = require("models/index");
import deleteIndexCommand = require("commands/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteIndexesConfirm extends dialogViewModelBase {

    deleteTask = $.Deferred();
    title: string;

    constructor(private indexNames: string[], private db: database) {
        super();

        if (!indexNames || indexNames.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }

        this.title = indexNames.length === 1 ? 'Delete index?' : 'Delete indexes?';
    }

    deleteIndexes() {
        var deleteTasks = this.indexNames
            .map(name => new deleteIndexCommand(name, this.db).execute());

        $.when(deleteTasks).done(() => this.deleteTask.resolve());
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;