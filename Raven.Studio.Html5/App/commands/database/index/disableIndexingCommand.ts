import commandBase = require("commands/commandBase");

class disableIndexingCommand extends commandBase {
    /**
    * @param names - The array of resource names to toggle
    * @param isSettingDisabled - Status of disabled to set
    * @param resourceType - The resource type
    */
    constructor(private dbName: string, private isSettingIndexingDisabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = !this.isSettingIndexingDisabled ? "enable" : "disable"; 
        var args = {
            isSettingIndexingDisabled: this.isSettingIndexingDisabled
        }
        var url = "/admin/databases/toggle-indexing/" + this.dbName + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Succefully " + action + "d " + " indexing in "+ this.dbName ));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " indexing in " + this.dbName, response.responseText, response.statusText));
        return toggleTask;
    }
}

export = disableIndexingCommand;  