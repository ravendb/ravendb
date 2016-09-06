import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require('commands/operations/getOperationStatusCommand');

class ioTestCommand extends commandBase {

    operationIdTask = $.Deferred();

    constructor(private db: database, private testParameters: performanceTestRequestDto, private onStatus: (state: operationStateDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var url = '/admin/ioTest';//TODO: use endpoints
        this.post(url, ko.toJSON(this.testParameters), null)
            .done((result: operationIdDto) => {
                this.operationIdTask.resolve(result.OperationId);
                this.monitorIoTest(promise, result.OperationId);
                })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to start disk IO test!", response.responseText, response.statusText);
                promise.reject();
            });
        return promise;
    }

    private monitorIoTest(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(null, operationId)
            .execute()
            .done((result: operationStatusDto) => {
                if (result.Completed) {
                    if (result.Faulted || result.Canceled) {
                        this.reportError("Failed to perform disk IO test!", result.State.Error);
                        parentPromise.reject();
                    } else {
                        this.reportSuccess("Disk IO test completed");   
                        parentPromise.resolve();
                    }
                } else {
                    this.onStatus(result.State);
                    setTimeout(() => this.monitorIoTest(parentPromise, operationId), 500);
                }
            });
    }
}

export = ioTestCommand;
