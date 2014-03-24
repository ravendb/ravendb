import index = require("models/index");
import deleteIndexCommand = require("commands/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class viewSystemDatabaseConfirm extends dialogViewModelBase {

    public viewTask = $.Deferred();
    
    
    constructor(private previousDb:database = null) {
        super();
    }

    viewSystemDatabase() {
        this.viewTask.resolve();
        dialog.close(this);
    }

    cancel() {
        this.viewTask.reject(this.previousDb);
        dialog.close(this);
    }
}

export = viewSystemDatabaseConfirm;