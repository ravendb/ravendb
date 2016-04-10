import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");
 
class performSmugglingCommand extends commandBase {

    constructor(private migration: serverSmugglingDto, private db: database, private updateMigrationStatus: (serverSmugglingOperationStateDto) => void) { 
        super();
    }

    execute(): JQueryPromise<any> {
        var result = $.Deferred();
        this.post("/admin/serverSmuggling", JSON.stringify(this.migration), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to perform server migration!", response.responseText, response.statusText);
                result.reject();
            })
            .done((operationId: operationIdDto) => {
                this.monitorOperation(result, operationId.OperationId);
            });
        return result;
    }

    private monitorOperation(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(appUrl.getDatabase(), operationId)
            .execute()
            .done((result: operationStatusDto) => {
            this.updateMigrationStatus(result);
            if (result.Completed) {
                if (result.Faulted) {
                    this.reportError("Failed to perform server migration!", result.State.Error);
                    parentPromise.reject();
                } else {
                    this.reportSuccess("Server migration completed");
                    parentPromise.resolve();
                }
            } else {
                setTimeout(() => this.monitorOperation(parentPromise, operationId), 500);
            }
        });
    }


}

export = performSmugglingCommand;
