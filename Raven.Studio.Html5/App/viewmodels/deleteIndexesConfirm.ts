import index = require("models/index");
import deleteIndexCommand = require("commands/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");

class deleteIndexesConfirm {

    public deleteTask = $.Deferred();

    constructor(private indexes: index[], private db: database) {
        if (!indexes || indexes.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }
    }

    deleteIndexes() {
        var deleteTasks = this.indexes
            .map(i => new deleteIndexCommand(i, this.db).execute());

        $.when(deleteTasks).done(() => this.deleteTask.resolve());
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;