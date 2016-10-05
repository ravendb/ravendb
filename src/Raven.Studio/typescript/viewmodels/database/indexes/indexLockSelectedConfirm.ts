import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import index = require("models/database/index/index");

class indexLockSelectedConfirm extends dialogViewModelBase {

    constructor(private setLockOperation: Raven.Abstractions.Indexing.IndexLockMode, private db: database, private indexes: index[], private message: string) {
        super();
    }

    indexLockIndexes() {
        this.indexes.forEach(i => {
            var originalLockMode = i.lockMode();
            if (originalLockMode !== this.setLockOperation) {
                i.lockMode(this.setLockOperation);
                new saveIndexLockModeCommand(i, this.setLockOperation, this.db)
                    .execute()
                    .fail(() => i.lockMode(originalLockMode));
            }
        });
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }
}

export = indexLockSelectedConfirm;
