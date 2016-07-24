import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class evalByQueryCommand extends commandBase {

    constructor(private indexName: string, private queryStr: string, private patchPayload: string, private db: database, private updatePatchingProgress: (bulkOperationStatusDto) => void) {
        super();
    }

    private operationId = $.Deferred<number>();
    private patchCompleted = $.Deferred<operationStatusDto>();

    /*
     * Promise returned by this method is initial request promise - this is resolved after opearation is scheduled (but NOT completed yet)
     */
    execute(): JQueryPromise<any> {
        this.reportInfo("Patching documents...");

        var url = "/bulk_docs/" + encodeURIComponent(this.indexName);
        var urlParams = "?query=" + encodeURIComponent(this.queryStr) + "&allowStale=true";
        var patchTask = this.evalJs(url + urlParams, this.patchPayload, this.db);
        // patch is made asynchronically so we infom user about operation start - not about actual completion. 
        patchTask.done((response: operationIdDto) => {
            this.reportSuccess("Scheduled patch of index: " + this.indexName);
            this.operationId.resolve(response.OperationId);
            this.monitorPatching(response.OperationId);
        });
        patchTask.fail((response: JQueryXHR) => this.reportError("Failed to schedule patch of index " + this.indexName, response.responseText, response.statusText));
        return patchTask;
    }

    private monitorPatching(operationId: number) {
        new getOperationStatusCommand(this.db, operationId)
            .execute()
            .done((result: operationStatusDto) => {
            this.updatePatchingProgress(result);

            if (result.Completed) {
                if (result.Faulted || result.Canceled) {
                    this.reportError("Patch failed", result.State.Error);
                    this.patchCompleted.reject();
                } else {
                    this.reportSuccess("Patching completed");
                    this.patchCompleted.resolve(result);
                }
            } else {
                setTimeout(() => this.monitorPatching(operationId), 500);
            }
        });
    }

    public getPatchCompletedTask() {
        return this.patchCompleted.promise();
    }

    public getPatchOperationId() {
        return this.operationId.promise();
    }
}

export = evalByQueryCommand; 
