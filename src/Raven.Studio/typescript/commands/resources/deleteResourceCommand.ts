import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

type deletionResult = {
    name: string;
    deleted: boolean;
    reason: string;
}

class deleteResourceCommand extends commandBase {
    
    private static readonly oneDatabasePath = endpoints.global.adminDatabases.adminDatabases;

    //TODO: use endpoints!
    private multipleDatabasesPath = "/admin/databases/batch-delete";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs/batch-delete";
    private oneCounterStoragePath = "/admin/cs/";
    private multipleCounterStoragesPath = "/admin/cs/batch-delete";
    private oneTimeSeriesPath = "/admin/ts/";
    private multipleTimeSeriesPath = "/admin/ts/batch-delete";

    constructor(private resources: Array<resource>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<Array<resource>> {
        if (this.resources.length === 1) {

            const task = $.Deferred<Array<resource>>();
            this.deleteOneResource()
                .done(result => {
                    if (result[0].deleted) {
                        task.resolve(this.resources);
                    } else {
                        task.reject(result[0].reason);
                    }
                })
                .fail(reason => task.reject(reason));
            return task;
        } else {
            throw new Error("not supported yet!");
        }
    }

    private deleteOneResource(): JQueryPromise<Array<deletionResult>> {
        const resource = this.resources[0];
        this.reportInfo("Deleting " + resource.name + "...");

        const args = {
            name: resource.name,
            "hard-delete": this.isHardDelete
        };

        const disableOneResourcePath = deleteResourceCommand.oneDatabasePath;
            /* TODO:(resource.type === TenantType.Database) ? this.oneDatabasePath :
            resource.type === TenantType.FileSystem ? this.oneFileSystemPath :
            resource.type === TenantType.CounterStorage ? this.oneCounterStoragePath : this.oneTimeSeriesPath;*/
        const url = disableOneResourcePath + this.urlEncodeArgs(args);
        return this.del(url, null)
            .done(() => this.reportSuccess("Successfully deleted " + resource.name))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete " + resource.name, response.responseText, response.statusText));
    }

    private deleteMultipleResources(): JQueryPromise<any> {
        throw new Error("not supported for now!");
        /* TODO:
        var _arguments = arguments;

        this.reportInfo("Deleting " + this.resources.length + " resources...");

        var dbToDelete = this.resources.filter(r => r.type === TenantType.Database);
        var fsToDelete = this.resources.filter(r => r.type === TenantType.FileSystem);
        var csToDelete = this.resources.filter(r => r.type === TenantType.CounterStorage);
        var tsToDelete = this.resources.filter(r => r.type === TenantType.TimeSeries);

        var deleteTasks: Array<JQueryDeferred<resource[]>> = [];

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
        return mergedPromise;*/
    }

    /* TODO:
    private deleteTask(resources: Array<resource>, deletePath: string) {
        var _arguments = arguments;

        var args = {
            ids: resources.map(d => d.name),
            "hard-delete": this.isHardDelete
        };

        var url = deletePath + this.urlEncodeArgs(args);

        var task = $.Deferred<resource[]>();
        this.del(url, null, null, null, 9000 * resources.length)
            .done((resourceNames: string[]) => {
                task.resolve(resources.filter(r => resourceNames.contains(r.name)));
            })
            .fail(() => task.reject(_arguments));
        return task;
    }*/

} 

export = deleteResourceCommand;
