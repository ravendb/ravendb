import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");

class deleteCollectionCommand extends commandBase {

    constructor(private collectionName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deleteTask = this.ravenDb.deleteCollection(this.collectionName);

        this.reportInfo("Deleting " + this.collectionName + " collection...");

        deleteTask.done(() => this.reportSuccess("Deleted " + this.collectionName + " collection"));
        deleteTask.fail((response) => this.reportError("Failed to delete collection", JSON.stringify(response)));
        return deleteTask;
    }
}

export = deleteCollectionCommand;