import commandBase = require("commands/commandBase");
import database = require("models/database");
import collection = require("models/collection");

class getOperationStatusCommand extends commandBase {

    /**
	* @param db - The database the collection belongs to.
	* @param operationId - The id of the operation.
	*/
    constructor(private db: database, private operationId: number) {
        super();

        if (!this.db) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collection[]> {
        var url = "/operation/status";

        var args = {
            id: this.operationId
        }

        return this.query(url, args, this.db);
    }
}

export = getOperationStatusCommand;