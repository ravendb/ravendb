import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");
import resource = require("models/resources/resource");
import counterStorage = require("models/counter/counterStorage");

class deleteDatabaseCommand extends commandBase {
    private oneDatabasePath = "/admin/databases/";
    private multipleDatabasesPath = "/admin/databases/batch-delete";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs/batch-delete";
    private oneCounterStoragePath = "/admin/cs/";
    private multipleCounterStoragesPath = "/admin/cs/batch-delete";

    constructor(private resources: Array<resource>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {

        var deleteTask: JQueryPromise<any>;
        if (this.resources.length === 1) {
            deleteTask = this.deleteOneResource();
        } else {
            deleteTask = this.deleteMultipleResources();
        }

        return deleteTask;
    }

    private deleteOneResource(): JQueryPromise<any> {
        var resource = this.resources[0];
        this.reportInfo("Deleting " + resource.name + "...");

        var args = {
            "hard-delete": this.isHardDelete
        };
        
        var disableOneResourcePath = (resource.type === TenantType.Database) ? this.oneDatabasePath :
            (resource.type == TenantType.FileSystem) ? this.oneFileSystemPath : this.oneCounterStoragePath;
        var url = disableOneResourcePath + encodeURIComponent(resource.name) + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null, null, { dataType: undefined });

        deleteTask.done(() => this.reportSuccess("Succefully deleted " + resource.name));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + resource.name, response.responseText, response.statusText));
        return deleteTask;
    }

    private deleteMultipleResources(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.resources.length + " resources...");

        var dbToDelete = this.resources.filter(r => r.type === TenantType.Database);
        var fsToDelete = this.resources.filter(r => r.type === TenantType.FileSystem);
        var cntToDelete = this.resources.filter(r => r.type === TenantType.CounterStorage);

        var deleteTasks = [];

        if (dbToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(dbToDelete, this.multipleDatabasesPath));
        }

        if (fsToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(fsToDelete, this.multipleFileSystemsPath));
        }

        if (cntToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(cntToDelete, this.multipleCounterStoragesPath));
        }

        var mergedPromise = $.Deferred();

        var combinedPromise = $.when.apply(null, deleteTasks);
        combinedPromise.done(() => {
            var deletedResources = [].concat.apply([], arguments);
            this.reportSuccess("Succefully deleted " + deletedResources.length + " resources!");
            mergedPromise.resolve(deletedResources);
        });

        combinedPromise.fail((response: JQueryXHR) => {
            this.reportError("Failed to delete resources", response.responseText, response.statusText);
            mergedPromise.reject(response);
        });
        return mergedPromise;
    }

    private deleteTask(resources: Array<resource>, deletePath: string) {
        var args = {
            ids: resources.map(d => d.name),
            "hard-delete": this.isHardDelete
        };

        var url = deletePath + this.urlEncodeArgs(args);

        var task = $.Deferred();
        this.del(url, null, null, null, 9000 * resources.length)
            .done((resourceNames: string[]) => {
                task.resolve(resources.filter(r => resourceNames.contains(r.name)));
            })
            .fail(() => task.reject(arguments));
        return task;
    }

} 

export = deleteDatabaseCommand;