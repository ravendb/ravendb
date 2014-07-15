import commandBase = require("commands/commandBase");
import database = require("models/database");

class toggleDatabaseDisabledCommand extends commandBase {

    /**
    * @param db - The array of database names to toggle
    * @param isSettingDisabled - The array of database names to toggle
    */
    constructor(private databaseNames: Array<string>, private isSettingDisabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.isSettingDisabled ? "disable" : "enable";

        var toggleTask;
        if (this.databaseNames.length == 1) {
            toggleTask = this.disableOneDatabse(action);
        } else {
            toggleTask = this.disableMultipleDatabases(action);
        }

        return toggleTask;
    }

    private disableOneDatabse(action: string): JQueryPromise<any> {
        var databaseName = this.databaseNames[0];
        this.reportInfo("Trying to " + action + " " + databaseName + "...");

        var args = {
            isSettingDisabled: this.isSettingDisabled
        };

        var url = "/admin/databases/" + databaseName + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Succefully " + action + "d " + databaseName));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + databaseName, response.responseText, response.statusText));
        
        return toggleTask;
    }

    private disableMultipleDatabases(action: string): JQueryPromise<any> {
        this.reportInfo("Trying to " + action + " " + this.databaseNames.length + " databases...");

        var args = {
            databaseIds: this.databaseNames,
            isSettingDisabled: this.isSettingDisabled
        };

        var url = "/admin/databases/database-batch-toggle-disable" + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null);
        toggleTask.done((toggledDatabaseNames: string[]) => this.reportSuccess("Succefully " + action + "d " + toggledDatabaseNames.length + " databases!"));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " databases", response.responseText, response.statusText));

        return toggleTask;
    }
}

export = toggleDatabaseDisabledCommand;  