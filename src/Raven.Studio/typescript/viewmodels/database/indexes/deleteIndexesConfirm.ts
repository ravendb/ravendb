import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");

class deleteIndexesConfirm extends dialogViewModelBase {

    deleteTask = $.Deferred<boolean>();
    title: string;

    constructor(private indexNames: string[], private db: database, title?: string) {
        super();

        if (!indexNames || indexNames.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }

        this.title = title || (indexNames.length === 1 ? "Delete index?" : "Delete indexes?");
    }

    deleteIndexes() {
        const deleteTasks = this.indexNames.map(name => new deleteIndexCommand(name, this.db).execute());

        $.when.apply($, deleteTasks)
            .done(() => {
                if (this.indexNames.length > 1) {
                    messagePublisher.reportSuccess("Successfully deleted " + this.indexNames.length + " indexes!");
                }
                this.deleteTask.resolve(true);
            })
            .fail(() => this.deleteTask.reject());
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.resolve(false);
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;
