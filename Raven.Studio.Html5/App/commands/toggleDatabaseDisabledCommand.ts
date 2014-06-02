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
        var args = {
            isSettingDisabled: !this.db.disabled()
        };

        var url = "/admin/databases/" + this.db.name + this.urlEncodeArgs(args);
        return this.post(url, null, null, { dataType: undefined });
    }
}

export = toggleDatabaseDisabledCommand;  