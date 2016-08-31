import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");

class getOperationStatusCommand extends commandBase {

    /**
    * @param db - The database the collection belongs to.
    * @param operationId - The id of the operation.
    */
    constructor(private rs: resource, private operationId: number) {
        super();

        if (!this.rs) {
            throw new Error("Must specify a resource.");
        }
    }

    execute(): JQueryPromise<any> {
        var url = "/operation/status";//TODO: use endpoints

        var args = {
            id: this.operationId
        }

        return this.query(url, args, this.rs);
    }
}

export = getOperationStatusCommand;
