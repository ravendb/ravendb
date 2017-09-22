import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getNextOperationId extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<number> {
        const url = this.db ? endpoints.databases.operations.operationsNextOperationId : endpoints.global.operationsServer.operationsNextOperationId;
        return this.query(url, null, this.db, x => x.Id);
    }
}

export = getNextOperationId; 
