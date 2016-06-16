import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import index = require("models/database/index/index");

class indexLockAllConfirm extends dialogViewModelBase {

    constructor(private setLockOperation: string, private db: database, private allIndexes: index[], private message: string) {
        super();
    }

    indexLockAllIndex() {
        this.allIndexes.forEach(i => {
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

export = indexLockAllConfirm;
