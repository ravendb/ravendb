import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class finishRollingCommand extends commandBase {
    constructor(private db: database, private indexName: string, private nodeTag: string = null) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            node: this.nodeTag || undefined
        };
        const url = endpoints.databases.index.indexesFinishRolling + this.urlEncodeArgs(args);
        
        const nodeText = this.nodeTag ? ` (on node: ${this.nodeTag})` : "";

        return this.post<void>(url, null, this.db)
            .done(() => this.reportSuccess("Finished rolling for index: " + this.indexName + nodeText));
    }
}

export = finishRollingCommand;
