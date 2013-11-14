import commandBase = require("commands/commandBase");

class deleteDocumentsCommand extends commandBase {

    constructor(private docIds: Array<string>) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deleteTask = this.ravenDb.deleteDocuments(this.docIds);

        var docCount = this.docIds.length;
        var alertInfoTitle = docCount > 1 ? "Deleting " + docCount + "docs..." : "Deleting " + this.docIds[0];
        this.reportInfo(alertInfoTitle);

        deleteTask.done(() => this.reportSuccess("Deleted " + docCount + " docs"));
        deleteTask.fail((response) => this.reportError("Failed to delete docs", JSON.stringify(response)));

        return deleteTask;
    }
}

export = deleteDocumentsCommand;