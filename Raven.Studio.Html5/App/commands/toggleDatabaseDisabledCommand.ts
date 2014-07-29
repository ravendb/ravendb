import commandBase = require("commands/commandBase");

class toggleDatabaseDisabledCommand extends commandBase {
    private disableOneDatabasePath = "/admin/databases/";
    private disableMultipleDatabasesPath = "/admin/databases/database-batch-toggle-disable";
    private disableOneFileSystemPath = "/admin/filesystems/";
    private disableMultipleFileSystemsPath = "/admin/filesystems/filesystem-batch-toggle-disable";

    /**
    * @param db - The array of database names to toggle
    * @param isSettingDisabled - The array of database names to toggle
    */
    constructor(private names: Array<string>, private isSettingDisabled: boolean, private disableOneResourcePath: string, private disableMultipleResourcesPath: string) {
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

        var url = this.disableOneResourcePath + name + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Succefully " + action + "d " + name));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + name, response.responseText, response.statusText));
        
        return toggleTask;
    }

    private disableMultipleResources(action: string): JQueryPromise<any> {
        this.reportInfo("Trying to " + action + " " + this.names.length + " databases...");

        var args = {
            ids: this.names,
            isSettingDisabled: this.isSettingDisabled
        };

        var url = this.disableMultipleResourcesPath + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null);
        toggleTask.done((toggledDatabaseNames: string[]) => this.reportSuccess("Succefully " + action + "d " + toggledDatabaseNames.length + " databases!"));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " databases", response.responseText, response.statusText));

        return toggleTask;
    }
}

export = toggleDatabaseDisabledCommand;  