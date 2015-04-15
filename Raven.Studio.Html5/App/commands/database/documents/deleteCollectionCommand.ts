import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class deleteCollectionCommand extends commandBase {
    private displayCollectionName: string;

    constructor(private collectionName: string, private db: database) {
        super();

        this.displayCollectionName = (collectionName == "*") ? "All Documents" : collectionName;
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.displayCollectionName);

        var url = "/bulk_docs/Raven/DocumentsByEntityName";
        var urlParams = "?query=Tag%3A" + encodeURIComponent(this.collectionName) + "&allowStale=true";
        var deleteTask = this.del(url + urlParams, null, this.db);
        // deletion is made asynchronically so we infom user about operation start - not about actual completion. 
        deleteTask.done(() => this.reportSuccess("Scheduled deletion of " + this.displayCollectionName));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + this.displayCollectionName, response.responseText, response.statusText));
        return deleteTask;
    }
}

export = deleteCollectionCommand; 