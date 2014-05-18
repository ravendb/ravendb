import commandBase = require("commands/commandBase");
import database = require("models/database");

class deleteDatabaseCommand extends commandBase {
    constructor(private databaseName: string, private isHardDelete: boolean, private systemDb: database) {
        super();
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Deleting " + this.databaseName + "...");

        var url = "/admin/databases/" + encodeURIComponent(this.databaseName) + "?hard-delete=" + this.isHardDelete;
        var deleteTask = this.del(url, null, this.systemDb, { dataType: undefined });
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete database", response.responseText, response.statusText));
        deleteTask.done(() => this.reportSuccess("Deleted " + this.databaseName));
        return deleteTask;
    }
} 

export = deleteDatabaseCommand;