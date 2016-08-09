import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");

class deleteIndexesConfirm extends dialogViewModelBase {

    deleteTask = $.Deferred();
    title: string;

    constructor(private indexNames: string[], private db: database, title?, private isDeleting: KnockoutObservable<boolean> = null) {
        super();

        if (!indexNames || indexNames.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }

        this.title = !!title ? title : indexNames.length === 1 ? "Delete index?" : "Delete indexes?";
    }

    deleteIndexes() {
        if (!!this.isDeleting) {
            this.isDeleting(true);
        }

        var deleteTasks = this.indexNames.map(name => new deleteIndexCommand(name, this.db).execute());
        var myDeleteTask = this.deleteTask;

        $.when.apply($, deleteTasks)
            .done(() => {
                if (this.indexNames.length > 1) {
                    messagePublisher.reportSuccess("Successfully deleted " + this.indexNames.length + " indexes!");
                }
                myDeleteTask.resolve(false);
            })
            .fail(() => {
                myDeleteTask.reject();
            })
            .always(() => {
                if (!!this.isDeleting) {
                    this.isDeleting(false);
                }
            });

        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;
