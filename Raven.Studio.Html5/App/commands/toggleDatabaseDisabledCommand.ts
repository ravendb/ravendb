import commandBase = require("commands/commandBase");
import database = require("models/database");

class toggleDatabaseDisabledCommand extends commandBase {

    /**
    * @param database - The database to toggle
    */
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.db.disabled() ? "enable" : "disable";
        var dbName = this.db.name;
        this.reportInfo("Trying to " + action + " " + dbName + "...");

        var args = {
            isSettingDisabled: !this.db.disabled()
        };

        var url = "/admin/databases/" + dbName + this.urlEncodeArgs(args);
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed to " + action + " " + dbName, response.responseText, response.statusText));
        toggleTask.done(() => this.reportSuccess(action.charAt(0).toUpperCase() + action.slice(1) + "d " + dbName));

        return toggleTask;
    }
}

export = toggleDatabaseDisabledCommand;  