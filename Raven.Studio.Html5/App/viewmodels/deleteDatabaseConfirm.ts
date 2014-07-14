import deleteDatabaseCommand = require("commands/deleteDatabaseCommand");
import database = require("models/database");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import router = require("plugins/router");

class deleteDatabaseConfirm extends dialogViewModelBase {
    private isKeepingFiles = ko.observable(true);
    private databasesToDelete = ko.observableArray<database>();
    public deleteTask = $.Deferred();
    //dbToDelete = ko.observable<database>();

    constructor(databases: Array<database>) {
        super();

        if (databases.length === 0) {
            throw new Error("Must have at least one database to delete.");
        }

        this.databasesToDelete(databases);
    }

    navigateToExportDatabase() {
        dialog.close(this);
        router.navigate(appUrl.forExportDatabase(this.databasesToDelete()[0]));
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteDatabase() {
        var databaseNames = this.databasesToDelete().map(db => db.name);
        new deleteDatabaseCommand(databaseNames, this.isKeepingFiles() === false)
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