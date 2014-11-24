import commandBase = require("commands/commandBase");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class disableResourceToggleCommand extends commandBase {
    private oneDatabasePath = "/admin/databases/";
    private multipleDatabasesPath = "/admin/databases/batch-toggle-disable";
    private oneFileSystemPath = "/admin/fs/";
    private multipleFileSystemsPath = "/admin/fs/batch-toggle-disable";
    private oneCounterStoragePath = "/admin/counterstorage/";
    private multipleCounterStoragesPath = "/admin/counterstorage/batch-toggle-disable";

    /**
    * @param names - The array of resource names to toggle
    * @param isSettingDisabled - Status of disabled to set
    * @param resourceType - The resource type
    */
    constructor(private resourcesNames: Array<string>, private isSettingDisabled: boolean, private resourceType: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.isSettingDisabled ? "disable" : "enable";

        var toggleTask;
        if (this.resourcesNames.length == 1) {
            toggleTask = this.disableOneResource(action);
        } else {
            toggleTask = this.disableMultipleResources(action);
        }

        return toggleTask;
    }

    private disableOneResource(action: string): JQueryPromise<any> {
        var name = this.resourcesNames[0];
        this.reportInfo("Trying to " + action + " " + name + "...");

        var args = {
            isSettingDisabled: this.isSettingDisabled
        };

        var disableOneResourcePath = (this.resourceType == database.type) ? this.oneDatabasePath :
            (this.resourceType == filesystem.type) ? this.oneFileSystemPath : this.oneCounterStoragePath;
        var url = disableOneResourcePath + name + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });

        toggleTask.done(() => this.reportSuccess("Succefully " + action + "d " + name));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + name, response.responseText, response.statusText));
        
        return toggleTask;
    }

    private disableMultipleResources(action: string): JQueryPromise<any> {
        var resourcesType = (this.resourceType == database.type) ? "databases" : (this.resourceType == filesystem.type) ? "file systems" : "counter storages";
        this.reportInfo("Trying to " + action + " " + this.resourcesNames.length + " " + resourcesType + "...");

        var args = {
            ids: this.resourcesNames,
            isSettingDisabled: this.isSettingDisabled
        };

        var disableMultipleResourcesPath = (this.resourceType == database.type) ? this.multipleDatabasesPath :
            (this.resourceType == filesystem.type) ? this.multipleFileSystemsPath : this.multipleCounterStoragesPath;
        var url = disableMultipleResourcesPath + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, null, 9000 * this.resourcesNames.length);

        toggleTask.done((toggledResourcesNames: string[]) => this.reportSuccess("Succefully " + action + "d " + toggledResourcesNames.length + " " + resourcesType + "!"));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + resourcesType, response.responseText, response.statusText));

        return toggleTask;
    }
}

export = disableResourceToggleCommand;  