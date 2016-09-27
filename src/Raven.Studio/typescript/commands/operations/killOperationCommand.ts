import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class killOperationCommand extends commandBase {

    constructor(private db: database, private taskId: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.taskId
        }
        const url = endpoints.databases.operations.operationKill + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db);
    }
}

export = killOperationCommand;
