import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");

class disableResourceToggleCommand extends commandBase {
    /**
    * @param resources - The array of resources to toggle
    * @param isDisabled - Status of disabled to set
    */
    constructor(private resources: Array<resource>, private isDisabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.isDisabled ? "disable" : "enable";

        var path = "/admin/";
        var resource = this.resources[0];
        switch (resource.type) {
        case TenantType.Database:
            path += "databases";
            break;
        case TenantType.FileSystem:
            path += "fs";
            break;
        case TenantType.CounterStorage:
            path += "cs";
            break;
        case TenantType.TimeSeries:
            path += "tx";
            break;
        default:
            break;
        }
        path += "/toggle-disable";

        let isOneResource = this.resources.length === 1;
        if (isOneResource) {
            this.reportInfo("Trying to " + action + " " + resource.name + "...");
        } else {
            this.reportInfo("Trying to " + action + " " + this.resources.length + " resources...");
        }

        var args = {
            name: this.resources.map(d => d.name),
            isDisabled: this.isDisabled
        };
        var url = path + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined }, 9000 * this.resources.length);
        toggleTask.done(() => {
/* TODO: Read the success in the response object*/
            if (isOneResource) {
                this.reportSuccess("Successfully " + action + "d " + name);
            } else {
                this.reportSuccess("Successfully " + action + "d " + this.resources.length + " resources!");
            }
        });
        toggleTask.fail((response: JQueryXHR) => {
            if (isOneResource) {
                this.reportError("Failed to " + action + " " + name, response.responseText, response.statusText)
            } else {
                this.reportError("Failed to " + action + " resources", response.responseText, response.statusText);
            }
        });
        return toggleTask;
    }
}

export = disableResourceToggleCommand;