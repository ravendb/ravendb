import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class getOperationCommand extends commandBase {

    constructor(private rs: resource, private operationId: number) {
        super();

        if (!this.rs) {
            throw new Error("Must specify a resource.");
        }
    }

    execute(): JQueryPromise<Raven.Server.Documents.PendingOperation> {
        const url = endpoints.databases.operations.operations;

        const args = {
            id: this.operationId
        }

        const extractor = (response: resultsDto<Raven.Server.Documents.PendingOperation>) => response.Results[0];

        return this.query(url, args, this.rs, extractor);
    }
}

export = getOperationCommand;
