import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import conflictsInfo = require("models/database/replication/conflictsInfo");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require('commands/operations/getOperationStatusCommand');

class conflictsResolveCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private resolution: string) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();

        var url = "/studio-tasks/replication/conflicts/resolve?resolution=" + this.resolution;//TODO: use endpoints
        this.post(url, null, this.ownerDb)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to start conflict resolution!", response.responseText, response.statusText);
                promise.reject();
            }).done((result: operationIdDto) => {
                this.monitorOperation(promise, result.OperationId);
            });

            return promise;
    }


    private monitorOperation(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(this.ownerDb, operationId)
            .execute()
            .done((result: operationStatusDto) => {
            if (result.Completed) {
                if (result.Faulted || result.Canceled) {
                    this.reportError("Failed to perform conflict resolution!", result.State.Error);
                    parentPromise.reject();
                } else {
                    this.reportSuccess("Conflict resolution was completed");
                    parentPromise.resolve();
                }
            } else {
                setTimeout(() => this.monitorOperation(parentPromise, operationId), 500);
            }
        });
    }
}

export = conflictsResolveCommand;
