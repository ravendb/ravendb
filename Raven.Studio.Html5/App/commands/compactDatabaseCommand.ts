import commandBase = require("commands/commandBase");
import database = require("models/database");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require('commands/getOperationStatusCommand');

class compactDatabaseCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var url = '/admin/compact' + this.urlEncodeArgs({ database: this.db.name });
        this.post(url, null, appUrl.getSystemDatabase())
            .done((result: operationIdDto) => this.monitorCompact(promise, result.OperationId))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to compact database!", response.responseText, response.statusText);
                promise.reject();
            });
        return promise;
    }

    private monitorCompact(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(appUrl.getSystemDatabase(), operationId)
            .execute()
            .done((result: operationStatusDto) => {
                if (result.Completed) {
                    if (result.Faulted) {
                        this.reportError("Failed to compact database!", result.State.Error);
                        parentPromise.reject();
                    } else {
                        this.reportSuccess("Compact completed");
                        parentPromise.resolve();
                    }
                } else {
                    setTimeout(() => this.monitorCompact(parentPromise, operationId), 500);
                }
            });
    }
}

export = compactDatabaseCommand;