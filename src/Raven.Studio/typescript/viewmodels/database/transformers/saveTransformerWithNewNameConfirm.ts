import saveTransformerCommand = require("commands/database/transformers/saveTransformerCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import transformer = require("models/database/index/transformer");
import messagePublisher = require("common/messagePublisher");

class saveTransformerWithNewNameConfirm extends dialogViewModelBase {

    saveTask = $.Deferred<void>();
    message: string;

    constructor(private savedTransformer: transformer, private db: database) {
        super();

        if (!savedTransformer) {
            throw new Error("Transformer must not be null");
        }

        this.message = "If you wish to save a new transformer with this new name press OK, to cancel the save command press Cancel";
    }

    saveTransformer() {
        new saveTransformerCommand(this.savedTransformer, this.db).execute().done(() => this.saveTask.resolve());
        dialog.close(this);
    }

    cancel() {
        this.saveTask.reject();
        messagePublisher.reportInfo("Transformer Not Saved!");
        dialog.close(this);
    }
}

export = saveTransformerWithNewNameConfirm; 
