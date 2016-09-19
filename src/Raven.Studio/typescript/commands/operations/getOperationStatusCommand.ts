import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class getOperationStatusCommand extends commandBase {

    constructor(private rs: resource, private operationId: number) {
        super();

        if (!this.rs) {
            throw new Error("Must specify a resource.");
        }
    }

    execute(): JQueryPromise<Raven.Client.Data.OperationState> {
        const url = endpoints.databases.operations.operationsStatus;

        const args = {
            id: this.operationId
        }

        return this.query(url, args, this.rs);
    }
}

export = getOperationStatusCommand;
