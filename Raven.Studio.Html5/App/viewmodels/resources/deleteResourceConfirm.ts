import deleteResourceCommand = require("commands/deleteResourceCommand");
import resource = require("models/resource");
import database = require("models/database");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteResourceConfirm extends dialogViewModelBase {
    private isKeepingFiles = ko.observable(true);
    private resourcesToDelete = ko.observableArray<resource>();
    public deleteTask = $.Deferred();
    isDeletingDatabase: boolean;
    exportDatabaseUrl: string;

    constructor(resources: Array<resource>) {
        super();

        if (resources.length === 0) {
            throw new Error("Must have at least one resource to delete.");
        }

        this.resourcesToDelete(resources);
        this.isDeletingDatabase = resources[0] instanceof database;
        if (this.isDeletingDatabase) {
            this.exportDatabaseUrl = appUrl.forExportDatabase(<any>resources[0]);
        }
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteDatabase() {
        new deleteResourceCommand(this.resourcesToDelete(), this.isKeepingFiles() === false)
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

export = deleteResourceConfirm;