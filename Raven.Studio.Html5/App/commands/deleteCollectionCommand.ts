import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

class deleteCollectionCommand extends commandBase {

    constructor(private collectionName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.collectionName + " collection...");

        var url = "/bulk_docs/Raven/DocumentsByEntityName";
        var urlParams = "?query=Tag%3A" + encodeURIComponent(this.collectionName) + "&allowStale=true";
        var deleteTask = this.del(url + urlParams, null, this.db);
        // deletion is made asynchronically so we infom user about operation start - not about actual completion. 
        deleteTask.done(() => this.reportSuccess("Scheduled deletion of " + this.collectionName));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete collection", response.responseText, response.statusText));
        return deleteTask;
    }
}

export = deleteCollectionCommand; 