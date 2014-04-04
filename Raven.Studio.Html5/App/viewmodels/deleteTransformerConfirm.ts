import index = require("models/index");
import deleteTransformerCommand = require("commands/deleteTransformerCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteTransformerConfirm extends dialogViewModelBase {

    deleteTask = $.Deferred();
    message: string;

    constructor(private transformersNames: string[], private db: database) {
        super();

        if (!transformersNames || transformersNames.length === 0) {
            throw new Error("Transformers must not be null or empty.");
        }

        //this.message = transformersNames.length === 1 ? "You're deleting  "Really Delete '" + transformersNames[0] + "' transformer?" : 'Really delete all transformers?';
    }

    deleteTransformers() {
        var deleteTasks = this.transformersNames
            .map(name => new deleteTransformerCommand(name, this.db).execute());

        $.when(deleteTasks).done(() => this.deleteTask.resolve());
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteTransformerConfirm; 