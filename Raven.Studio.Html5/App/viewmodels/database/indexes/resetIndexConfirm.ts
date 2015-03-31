import index = require("models/index");
import resetIndexCommand = require("commands/resetIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class resetIndexConfirm extends dialogViewModelBase {

    resetTask = $.Deferred();

    constructor(private indexName: string, private db: database) {
        super();
    }

    resetIndex() {
        new resetIndexCommand(this.indexName, this.db).execute().done(() => this.resetTask.resolve()).fail(() => this.resetTask.reject());
        dialog.close(this);
    }

    cancel() {
        this.resetTask.reject();
        dialog.close(this);
    }
}

export = resetIndexConfirm;