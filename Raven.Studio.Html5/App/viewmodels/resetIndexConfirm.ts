import index = require("models/index");
import resetIndexCommand = require("commands/resetIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class resetIndexConfirm extends dialogViewModelBase {

    resetTask = $.Deferred();
    message:string;

    constructor(private indexName: string, private db: database) {
        super();

        this.message = 'Are you sure that you want to reset this index ' + indexName + '?';
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