import resetIndexCommand = require("commands/database/index/resetIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class resetIndexConfirm extends dialogViewModelBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    resetIndex(): void {
        new resetIndexCommand(this.indexName, this.db)
            .execute();
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }
}

export = resetIndexConfirm;
