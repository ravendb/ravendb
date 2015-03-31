import commandBase = require("commands/commandBase");

class toggleRejectDatabaseClients extends commandBase {
    /**
    * @param names - The array of resource names to toggle
    * @param isSettingDisabled - Status of disabled to set
    * @param resourceType - The resource type
    */
    constructor(private dbName: string, private rejectClientsEnabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.rejectClientsEnabled ? "reject clients mode" : "accept clients mode";
        var args = {
            isRejectClientsEnabled: this.rejectClientsEnabled
        }
        var url = "/admin/databases/toggle-reject-clients/" + this.dbName + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Successfully switched to " + action + " in " + this.dbName));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed switching to " + action + " in " + this.dbName, response.responseText, response.statusText));
        return toggleTask;
    }
}

export = toggleRejectDatabaseClients;   