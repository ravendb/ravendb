import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");

class getNextOperationId extends commandBase {

    constructor(private rs: resource) {
        super();
    }

    execute() {
        var operationId = this.query("/studio-tasks/next-operation-id", null, this.rs);
        return operationId;
    }
}

export = getNextOperationId; 
