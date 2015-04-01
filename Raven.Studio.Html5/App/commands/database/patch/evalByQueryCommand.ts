import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class evalByQueryCommand extends commandBase {

    constructor(private indexName: string, private queryStr: string, private patchPayload: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Patching documents...");

        var url = "/bulk_docs/" + encodeURIComponent(this.indexName);
        var urlParams = "?query=" + encodeURIComponent(this.queryStr) + "&allowStale=true";
        var patchTask = this.evalJs(url + urlParams, this.patchPayload, this.db);
        // patch is made asynchronically so we infom user about operation start - not about actual completion. 
        patchTask.done(() => this.reportSuccess("Scheduled patch of index" + this.indexName));
        patchTask.fail((response: JQueryXHR) => this.reportError("Failed to schedule patch of index " + this.indexName, response.responseText, response.statusText));
        return patchTask;
    }
}

export = evalByQueryCommand; 