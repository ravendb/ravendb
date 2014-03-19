import index = require("models/index");
import saveTransformerCommand = require("commands/saveTransformerCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import transformer = require("models/transformer");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");

class saveTransformerWithNewNameConfirm extends dialogViewModelBase {

    saveTask= $.Deferred();
    message: string;

    constructor(private savedTransformer: transformer, private db: database) {
        super();

        if (!savedTransformer) {
            throw new Error("Transformer must not be null");
        }

        this.message = "If you wish to save a new transformer with this new name press OK, to cancel the save command press Cancel";
    }

    saveTransformer() {
        new saveTransformerCommand(this.savedTransformer, this.db).execute().done((trans: transformer) => this.saveTask.resolve(trans));
        dialog.close(this);
    }

    cancel() {
        this.saveTask.reject();
        ko.postbox.publish("Alert", new alertArgs(alertType.info, "Transformer Not Saved"));
        dialog.close(this);
    }
}

export = saveTransformerWithNewNameConfirm; 