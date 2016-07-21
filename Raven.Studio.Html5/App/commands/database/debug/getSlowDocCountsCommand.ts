import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import debugDocumentStats = require("models/database/debug/debugDocumentStats");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class getSlowDocCountsCommand extends commandBase {

    constructor(private db: database, private updateProgress: (dto) => void) {
        super();
    }

    private calculationCompleted = $.Deferred<debugDocumentStats>();

    execute(): JQueryPromise<operationIdDto> {
        var url = "/debug/sl0w-d0c-c0unts";

        return this.query<operationIdDto>(url, null, this.db)
            .done((response) => {
                this.monitorOperation(response.OperationId);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to compute document counts", response.responseText, response.statusText));
    }

    private monitorOperation(operationId: number) {
        new getOperationStatusCommand(this.db, operationId)
            .execute()
            .done((result: debugDocumentStatsStateDto) => {
                this.updateProgress(result.State.Progress);

                if (result.Completed) {
                    if (result.Faulted || result.Canceled) {
                        this.reportError("Calculation failed", result.State.Error);
                        this.calculationCompleted.reject();
                    } else {
                        this.reportSuccess("Calculation completed");
                        this.calculationCompleted.resolve(new debugDocumentStats(result.Stats));
                    }
                } else {
                    setTimeout(() => this.monitorOperation(operationId), 500);
                }
            });
    }

    public getCalculationCompletedTask(): JQueryPromise<debugDocumentStats> {
        return this.calculationCompleted.promise();
    }

}

export = getSlowDocCountsCommand;
