import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class getStatusStorageBreakdownCommand extends commandBase {

    constructor(private db: database, private updateProgress: (dto) => void) {
        super();
    }

    private calculationCompleted = $.Deferred<Array<string>>();

    execute(): JQueryPromise<{ OperationId: number; }> {
        var url = "/admin/detailed-storage-breakdown";
        return this.query<any>(url, null, this.db)
            .done((response) => {
                this.monitorOperation(response.OperationId);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to compute internal storage breakdown", response.responseText, response.statusText));
    }

    private monitorOperation(operationId: number) {
        new getOperationStatusCommand(this.db, operationId)
            .execute()
            .done((result: internalStorageBreakdownState) => {
                this.updateProgress(result.State.Progress);

                if (result.Completed) {
                    if (result.Faulted || result.Canceled) {
                        this.reportError("Calculation failed", result.State.Error);
                        this.calculationCompleted.reject();
                    } else {
                        this.reportSuccess("Calculation completed");
                        this.calculationCompleted.resolve(result.ReportResults);
                    }
                } else {
                    setTimeout(() => this.monitorOperation(operationId), 500);
                }
            });
    }

    public getBreakdownCompletedTask() {
        return this.calculationCompleted.promise();
    }

}

export = getStatusStorageBreakdownCommand;
