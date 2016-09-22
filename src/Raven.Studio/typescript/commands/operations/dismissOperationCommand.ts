import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class dismissOperationCommand extends commandBase {

    constructor(private rs: resource, private operationId: number) {
        super();

        if (!this.rs) {
            throw new Error("Must specify a resource.");
        }
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.operations.operationDismiss;

        const args = {
            id: this.operationId
        }

        return this.query<void>(url, args, this.rs, null, { dataType: undefined });
    }
}

export = dismissOperationCommand;
