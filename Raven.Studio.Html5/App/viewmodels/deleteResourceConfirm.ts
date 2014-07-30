import deleteResourceCommand = require("commands/deleteResourceCommand");
import resource = require("models/resource");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import router = require("plugins/router");

class deleteResourceConfirm extends dialogViewModelBase {
    private isKeepingFiles = ko.observable(true);
    private resourcesToDelete = ko.observableArray<resource>();
    public deleteTask = $.Deferred();
    isDeletingDatabase: boolean;
    resourceType: string;
    resourcesTypeText: string;

    constructor(resources: Array<resource>) {
        super();

        if (resources.length === 0) {
            throw new Error("Must have at least one resource to delete.");
        }

        this.resourcesToDelete(resources);
        this.isDeletingDatabase = resources[0] instanceof database;
        this.resourceType = resources[0].type;
        this.resourcesTypeText = this.resourceType == database.type ? 'databases' : this.resourceType == filesystem.type ? 'file systems' : 'counter storages';
    }

    navigateToExportDatabase() {
        dialog.close(this);
        var db: any = this.resourcesToDelete()[0];
        router.navigate(appUrl.forExportDatabase(db));
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteDatabase() {
        var resourcesNames = this.resourcesToDelete().map((rs: resource) => rs.name);
        new deleteResourceCommand(resourcesNames, this.isKeepingFiles() === false, this.resourceType)
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