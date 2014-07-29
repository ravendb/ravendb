import commandBase = require("commands/commandBase");

class disableResourceToggleCommand extends commandBase {
    private disableOneDatabasePath = "/admin/databases/";
    private disableMultipleDatabasesPath = "/admin/databases/database-batch-toggle-disable";
    private disableOneFileSystemPath = "/admin/fs/";
    private disableMultipleFileSystemsPath = "/admin/fs/filesystem-batch-toggle-disable";

    /**
    * @param names - The array of resource names to toggle
    * @param isSettingDisabled - Status of disabled to set
    * @param resourceType - The resource type
    */
    constructor(private names: Array<string>, private isSettingDisabled: boolean, private resourceType: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.isSettingDisabled ? "disable" : "enable";

        var toggleTask;
        if (this.names.length == 1) {
            toggleTask = this.disableOneResource(action);
        } else {
            toggleTask = this.disableMultipleResources(action);
        }

        return toggleTask;
    }

    private disableOneResource(action: string): JQueryPromise<any> {
        var name = this.names[0];
        this.reportInfo("Trying to " + action + " " + name + "...");

        var args = {
            isSettingDisabled: this.isSettingDisabled
        };

        var disableOneResourcePath = (this.resourceType == "database") ? this.disableOneDatabasePath : this.disableOneFileSystemPath;
        var url = disableOneResourcePath + name + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Succefully " + action + "d " + name));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + name, response.responseText, response.statusText));
        
        return toggleTask;
    }

    private disableMultipleResources(action: string): JQueryPromise<any> {
        var resources = (this.resourceType == "database") ? "databases" : "file systems";
        this.reportInfo("Trying to " + action + " " + this.names.length + " " + resources + "...");

        var args = {
            ids: this.names,
            isSettingDisabled: this.isSettingDisabled
        };

        var disableMultipleResourcesPath = (this.resourceType == "database") ? this.disableMultipleDatabasesPath : this.disableMultipleFileSystemsPath;
        var url = disableMultipleResourcesPath + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null);
        toggleTask.done((toggledResourceNames: string[]) => this.reportSuccess("Succefully " + action + "d " + toggledResourceNames.length + " " + resources + "!"));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " databases", response.responseText, response.statusText));

        return toggleTask;
    }
}

export = disableResourceToggleCommand;  