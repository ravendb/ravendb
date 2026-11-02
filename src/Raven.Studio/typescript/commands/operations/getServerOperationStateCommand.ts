import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerOperationStateCommand extends commandBase {

    constructor(private operationId: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OperationState> {
        const args = {
            id: this.operationId
        }
        const url = endpoints.global.operationsServer.operationsState + this.urlEncodeArgs(args);

        return this.query<Raven.Client.Documents.Operations.OperationState>(url, null);
    }
}

export = getServerOperationStateCommand;
