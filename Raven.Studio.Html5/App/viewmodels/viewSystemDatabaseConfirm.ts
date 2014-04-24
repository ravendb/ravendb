import index = require("models/index");
import deleteIndexCommand = require("commands/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class viewSystemDatabaseConfirm extends dialogViewModelBase {

    public viewTask = $.Deferred();
    private wasConfirmed:boolean = false;
    
    
    constructor(private previousDb:database = null) {
        super();
    }

    viewSystemDatabase() {
        this.viewTask.resolve();
        this.wasConfirmed = true;
        dialog.close(this);
    }

    cancel() {
        this.viewTask.reject(this.previousDb);
        this.wasConfirmed = false;
        dialog.close(this);
    }

    detached() {
        super.detached();

        if (!this.wasConfirmed) {
            this.viewTask.reject(this.previousDb);
        }
    }
}

export = viewSystemDatabaseConfirm;