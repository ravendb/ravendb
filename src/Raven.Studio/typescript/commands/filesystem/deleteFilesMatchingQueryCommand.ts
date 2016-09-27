import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class deleteFilesMatchingQueryCommand extends commandBase {
    constructor(private queryText: string, private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<{ OperationId: number; }> {
        var deleteTaskWithWait = $.Deferred();
        this.reportInfo("Deleting files matching query...");

        var args = {
            query: this.queryText
        };

        var url = "/search/" + this.urlEncodeArgs(args);
        var task = this.del(url, null, this.fs);
        task.done((result) => this.waitForOperationToComplete(this.fs, result.OperationId, deleteTaskWithWait));
        task.fail((response: JQueryXHR) => this.reportError("Error deleting files matching query", response.responseText, response.statusText));
        return deleteTaskWithWait;
    }

    private waitForOperationToComplete(fs: filesystem, operationId: number, task: JQueryDeferred<any>) {
        new getOperationStatusCommand(fs, operationId)
            .execute();
            //TODO: .done((result: operationStatusDto) => this.deletionStatusRetrieved(fs, operationId, result, task));
    }

    private deletionStatusRetrieved(fs: filesystem, operationId: number, result: operationStatusDto, task: JQueryDeferred<any>) {
        if (result.Completed) {
            if (!result.Faulted && !result.Canceled) {
                this.reportSuccess("Files deleted");
                task.resolve();
            } else {
                this.reportError("Error deleting files matching query");
                task.reject();
            }
            fs.isImporting(false);
        }
        else {
            setTimeout(() => this.waitForOperationToComplete(fs, operationId, task), 1000);
        }
    }

}

export = deleteFilesMatchingQueryCommand; 
