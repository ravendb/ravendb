import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getNextOperationId extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<number> {
        return this.query(endpoints.databases.operations.operationsNextOperationId, null, this.db);
    }
}

export = getNextOperationId; 
