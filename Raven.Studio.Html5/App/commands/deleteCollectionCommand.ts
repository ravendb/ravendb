import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

class deleteCollectionCommand extends commandBase {

    constructor(private collectionName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.collectionName + " collection...");

        var args = {
            query: "Tag:" + this.collectionName,
            pageSize: 128,
            allowStale: true
        };
        var url = "/bulk_docs/Raven/DocumentsByEntityName";
        var urlParams = "?query=Tag%3A" + encodeURIComponent(this.collectionName) + "&pageSize=128&allowStale=true";
        var deleteTask = this.del(url + urlParams, null, this.db);
        deleteTask.done(() => this.reportSuccess("Deleted " + this.collectionName + " collection"));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete collection", response.responseText, response.statusText));
        return deleteTask;
    }
}

export = deleteCollectionCommand; 