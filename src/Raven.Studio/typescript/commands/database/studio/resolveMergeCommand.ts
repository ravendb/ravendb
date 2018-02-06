import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class resolveMergeCommand extends commandBase {

    constructor(private db: database, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<mergeResult> {
        var url = "/studio-tasks/resolveMerge";//TODO: use endpoints
        var args = {
            documentId: this.documentId
        };
        return this.query<mergeResult>(url, args, this.db);
    }

}

export = resolveMergeCommand;
