import commandBase = require("commands/commandBase");

class deleteCounterStorageCommand extends commandBase {
    constructor(private counterStorageName: string, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Deleting " + this.counterStorageName + "...");

        var url = "/counterstorage/admin/" + encodeURIComponent(this.counterStorageName) + "?hard-delete=" + this.isHardDelete;
        var deleteTask = this.del(url, null, null, { dataType: undefined });
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete counter storage", response.responseText, response.statusText));
        deleteTask.done(() => this.reportSuccess("Deleted " + this.counterStorageName));
        return deleteTask;
    }
}

export = deleteCounterStorageCommand;