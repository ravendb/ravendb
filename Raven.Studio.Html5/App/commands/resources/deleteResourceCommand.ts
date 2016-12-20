import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");

class deleteDatabaseCommand extends commandBase {
    private oneDatabasePath = "/admin/databases/";
    private multipleDatabasesPath = "/admin/databases-batch-delete";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs-batch-delete";
    private oneCounterStoragePath = "/admin/cs/";
    private multipleCounterStoragesPath = "/admin/cs/batch-delete";
    private oneTimeSeriesPath = "/admin/ts/";
    private multipleTimeSeriesPath = "/admin/ts/batch-delete";

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
            resource.type === TenantType.FileSystem ? this.oneFileSystemPath :
            resource.type === TenantType.CounterStorage ? this.oneCounterStoragePath : this.oneTimeSeriesPath;
        var url = disableOneResourcePath + encodeURIComponent(resource.name) + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null, null, { dataType: undefined });

        deleteTask.done(() => this.reportSuccess("Successfully deleted " + resource.name));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + resource.name, response.responseText, response.statusText));
        return deleteTask;
    }

    private deleteMultipleResources(): JQueryPromise<any> {
        var _arguments = arguments;

        this.reportInfo("Deleting " + this.resources.length + " resources...");

        var dbToDelete = this.resources.filter(r => r.type === TenantType.Database);
        var fsToDelete = this.resources.filter(r => r.type === TenantType.FileSystem);
        var csToDelete = this.resources.filter(r => r.type === TenantType.CounterStorage);
        var tsToDelete = this.resources.filter(r => r.type === TenantType.TimeSeries);

        var deleteTasks = [];

        if (dbToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(dbToDelete, this.multipleDatabasesPath));
        }

        if (fsToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(fsToDelete, this.multipleFileSystemsPath));
        }

        if (csToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(csToDelete, this.multipleCounterStoragesPath));
        }

        if (tsToDelete.length > 0) {
            deleteTasks.push(this.deleteTask(tsToDelete, this.multipleTimeSeriesPath));
        }

        var mergedPromise = $.Deferred();

        var combinedPromise = $.when.apply(null, deleteTasks);
        combinedPromise.done((...resources: resource[][]) => {
            var deletedResources = [].concat.apply([], resources);
            this.reportSuccess("Successfully deleted " + deletedResources.length + " resources!");
            mergedPromise.resolve(deletedResources);
        });

        combinedPromise.fail((response: JQueryXHR) => {
            this.reportError("Failed to delete resources", response.responseText, response.statusText);
            mergedPromise.reject(response);
        });
        return mergedPromise;
    }

    private deleteTask(resources: Array<resource>, deletePath: string) {
        var _arguments = arguments;

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
            .fail(() => task.reject(_arguments));
        return task;
    }

} 

export = deleteDatabaseCommand;
