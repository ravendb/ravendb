import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class getNextOperationId extends commandBase {

    constructor(private rs: resource) {
        super();
    }

    execute(): JQueryPromise<number> {
        return this.query(endpoints.databases.operations.operationsNextOperationId, null, this.rs);
    }
}

export = getNextOperationId; 
