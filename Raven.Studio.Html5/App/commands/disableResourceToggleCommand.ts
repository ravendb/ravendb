import commandBase = require("commands/commandBase");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import resource = require("models/resource");
import counterStorage = require("models/counter/counterStorage");

class disableResourceToggleCommand extends commandBase {
    private oneDatabasePath = "/admin/databases-toggle-disable";
    private multipleDatabasesPath = "/admin/databases/batch-toggle-disable";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs/batch-toggle-disable";
    private oneCounterStoragePath = "/admin/counterstorage/";
    private multipleCounterStoragesPath = "/admin/counterstorage/batch-toggle-disable";

    /**
    * @param resources - The array of resources to toggle
    * @param isSettingDisabled - Status of disabled to set
    */
    constructor(private resources: Array<resource>, private isSettingDisabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.isSettingDisabled ? "disable" : "enable";

        var toggleTask;
        if (this.resources.length == 1) {
            toggleTask = this.disableOneResource(action);
        } else {
            toggleTask = this.disableMultipleResources(action);
        }

        return toggleTask;
    }

    private disableOneResource(action: string): JQueryPromise<any> {
        var resource = this.resources[0];
        this.reportInfo("Trying to " + action + " " + resource.name + "...");

        var args = (resource.type === database.type) ? {
            id : resource.name,
            isSettingDisabled: this.isSettingDisabled
        } : {
            isSettingDisabled: this.isSettingDisabled
        };

        var disableOneResourcePath = (resource.type === database.type) ? this.oneDatabasePath :
            (resource.type === filesystem.type) ? this.oneFileSystemPath : this.oneCounterStoragePath;
        var resourceName = (resource.type === database.type) ? "" : resource.name; 
        var url = disableOneResourcePath + resourceName + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });

        toggleTask.done(() => this.reportSuccess("Successfully " + action + "d " + name));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + name, response.responseText, response.statusText));
        
        return toggleTask;
    }

    private disableMultipleResources(action: string): JQueryPromise<any> {
        this.reportInfo("Trying to " + action + " " + this.resources.length + " resources...");

        var dbToToggle = this.resources.filter(r => r.type === database.type);
        var fsToToggle = this.resources.filter(r => r.type === filesystem.type);
        var cntToToggle = this.resources.filter(r => r.type === counterStorage.type);

        var toggleTasks:Array<JQueryPromise<resource[]>> = [];

        if (dbToToggle.length > 0) {
            toggleTasks.push(this.toggleTask(dbToToggle, this.multipleDatabasesPath));
        }

        if (fsToToggle.length > 0) {
            toggleTasks.push(this.toggleTask(fsToToggle, this.multipleFileSystemsPath));
        }

        if (cntToToggle.length > 0) {
            toggleTasks.push(this.toggleTask(cntToToggle, this.multipleCounterStoragesPath));
        }

        var mergedPromise = $.Deferred();

        var combinedPromise = $.when.apply(null, toggleTasks);
        combinedPromise.done((resources) => {
            var toggledResources = [].concat.apply([], resources);
            this.reportSuccess("Successfully " + action + "d " + toggledResources.length + " resources!")
            mergedPromise.resolve(toggledResources);
        });

        combinedPromise.fail((response: JQueryXHR) => {
            this.reportError("Failed to " + action + " resources", response.responseText, response.statusText);
            mergedPromise.reject(response);
            });
        return mergedPromise;
    }

    private toggleTask(resources: Array<resource>, togglePath: string):JQueryPromise<resource[]> {
        var _arguments = arguments;

        var args = {
            ids: resources.map(d => d.name),
            isSettingDisabled: this.isSettingDisabled
        };

        var url = togglePath + this.urlEncodeArgs(args);

        var task = $.Deferred();
        this.post(url, null, null, null, 9000 * resources.length)
            .done((resourceNames: string[]) => {
                task.resolve(resources.filter(r => resourceNames.contains(r.name)));
            })
            .fail(() => task.reject(_arguments));
        return task;

        
    }

}

export = disableResourceToggleCommand;  
