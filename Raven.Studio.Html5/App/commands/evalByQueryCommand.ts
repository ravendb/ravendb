import commandBase = require("commands/commandBase");
import database = require("models/database");
import getOperationStatusCommand = require("commands/getOperationStatusCommand");

class evalByQueryCommand extends commandBase {

    constructor(private indexName: string, private queryStr: string, private patchPayload: string, private db: database) {
        super();
    }

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
            this.reportSuccess("Scheduled patch of index " + this.indexName);
            this.monitorPatching(response.OperationId);
        });
        patchTask.fail((response: JQueryXHR) => this.reportError("Failed to schedule patch of index " + this.indexName, response.responseText, response.statusText));
        return patchTask;
    }

    private monitorPatching(operationId: number) {
        new getOperationStatusCommand(this.db, operationId)
            .execute()
            .done((result: operationStatusDto) => {
            if (result.Completed) {
                if (result.Faulted) {
                    this.reportError("Patch failed", result.State.Error);
                } else {
                    this.reportSuccess("Patching completed");
                }
            } else {
                setTimeout(() => this.monitorPatching(operationId), 500);
            }
        });
    }
}

export = evalByQueryCommand; 
