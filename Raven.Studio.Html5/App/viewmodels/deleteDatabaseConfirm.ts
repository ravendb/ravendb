import deleteDatabaseCommand = require("commands/deleteDatabaseCommand");
import database = require("models/database");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");

class deleteDatabaseConfirm {
    private isKeepingFiles = ko.observable(true);
    private exportDbUrl = "";
    public deleteTask = $.Deferred();

    constructor(private dbToDelete: database, private systemDb: database) {
        if (!dbToDelete) {
            throw new Error("Must specified database to delete.");
        }

        if (!systemDb) {
            throw new Error("Must specify system database");
        }

        this.exportDbUrl = appUrl.forExport(dbToDelete);
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteDatabase() {
        new deleteDatabaseCommand(this.dbToDelete.name, this.isKeepingFiles() === false, this.systemDb)
            .execute()
            .done(results => this.deleteTask.resolve(results))
            .fail(details => this.deleteTask.reject(details));
        
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteDatabaseConfirm;