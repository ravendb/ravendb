import commandBase = require("commands/commandBase");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class deleteDatabaseCommand extends commandBase {
    private oneDatabasePath = "/admin/databases/";
    private multipleDatabasesPath = "/admin/databases/batch-delete";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs/batch-delete";
    private oneCounterStoragePath = "/admin/counterstorage/";
    private multipleCounterStoragesPath = "/admin/counterstorage/batch-delete";

    constructor(private resourcesNames: string[], private isHardDelete: boolean, private resourceType: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var deleteTask;
        if (this.resourcesNames.length == 1) {
            deleteTask = this.deleteOneResource();
        } else {
            deleteTask = this.deleteMultipleResources();
        }

        return deleteTask;
    }

    private deleteOneResource(): JQueryPromise<any> {
        var resourceName = this.resourcesNames[0];
        this.reportInfo("Deleting " + resourceName + "...");

        var args = {
            "hard-delete": this.isHardDelete
        };

        var disableOneResourcePath = (this.resourceType == database.type) ? this.oneDatabasePath :
            (this.resourceType == filesystem.type) ? this.oneFileSystemPath : this.oneCounterStoragePath;
        var url = disableOneResourcePath + encodeURIComponent(resourceName) + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null, null, { dataType: undefined });

        deleteTask.done(() => this.reportSuccess("Succefully deleted " + resourceName));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + resourceName, response.responseText, response.statusText));

        return deleteTask;
    }

    private deleteMultipleResources(): JQueryPromise<any> {
        var resourcesType = (this.resourceType == database.type) ? "databases" : (this.resourceType == filesystem.type) ? "file systems" : "counter storages";
        this.reportInfo("Deleting " + this.resourcesNames.length + " " + resourcesType + "...");

        var args = {
            ids: this.resourcesNames,
            "hard-delete": this.isHardDelete
        };

        var disableMultipleResourcesPath = (this.resourceType == database.type) ? this.multipleDatabasesPath :
            (this.resourceType == filesystem.type) ? this.multipleFileSystemsPath : this.multipleCounterStoragesPath;
        var url = disableMultipleResourcesPath + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null);

        deleteTask.done((deletedResourcesNames: string[]) => this.reportSuccess("Succefully deleted " + deletedResourcesNames.length + " " + resourcesType + "!"));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete "+ resourcesType, response.responseText, response.statusText));

        return deleteTask;
    }
} 

export = deleteDatabaseCommand;