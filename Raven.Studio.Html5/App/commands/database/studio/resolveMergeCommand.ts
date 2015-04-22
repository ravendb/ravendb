import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class resolveMergeCommand extends commandBase {

    constructor(private db: database, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<mergeResult> {
      var url = "/databases/" + this.db.name + "/studio-tasks/resolveMerge";
      var args = {
        documentId: this.documentId
      };
      var task = this.query<mergeResult>(url, args);
      return task;
    }

}

export = resolveMergeCommand;