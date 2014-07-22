import commandBase = require("commands/commandBase");
import database = require("models/database");

class deleteDatabaseCommand extends commandBase {
    constructor(private databaseNames: string[], private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {

        var deleteTask;
        if (this.databaseNames.length == 1) {
            deleteTask = this.deleteOneDatabse();
        } else {
            deleteTask = this.deleteMultipleDatabases();
        }

        return deleteTask;
    }

    private deleteOneDatabse(): JQueryPromise<any> {
        var databaseName = this.databaseNames[0];
        this.reportInfo("Deleting " + databaseName + "...");

        var url = "/admin/databases/" + encodeURIComponent(databaseName) + "?hard-delete=" + this.isHardDelete;
        var deleteTask = this.del(url, null, null, { dataType: undefined });
        deleteTask.done(() => this.reportSuccess("Succefully deleted " + databaseName));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete database " + databaseName, response.responseText, response.statusText));

        return deleteTask;
    }

    private deleteMultipleDatabases(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.databaseNames.length + " databases...");

        var args = {
            databaseIds: this.databaseNames,
            "hard-delete": this.isHardDelete
        };

        var url = "/admin/databases/database-batch-delete" + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null);
        deleteTask.done((deletedDatabaseNames: string[]) => this.reportSuccess("Succefully deleted " + deletedDatabaseNames.length + " databases!"));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete databases", response.responseText, response.statusText));

        return deleteTask;
    }
} 

export = deleteDatabaseCommand;