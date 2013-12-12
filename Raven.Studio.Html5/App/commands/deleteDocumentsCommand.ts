import commandBase = require("commands/commandBase");
import database = require("models/database");

class deleteDocumentsCommand extends commandBase {

    constructor(private docIds: Array<string>, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deleteDocs = this.docIds.map(id => this.createDeleteDocument(id));
        var deleteTask = this.post("/bulk_docs", ko.toJSON(deleteDocs), this.db);
        
        var docCount = this.docIds.length;
        var alertInfoTitle = docCount > 1 ? "Deleting " + docCount + "docs..." : "Deleting " + this.docIds[0];
        this.reportInfo(alertInfoTitle);

        deleteTask.done(() => this.reportSuccess("Deleted " + docCount + " docs"));
        deleteTask.fail((response) => this.reportError("Failed to delete docs", JSON.stringify(response)));

        return deleteTask;
    }

    private createDeleteDocument(id: string) {
        return {
            Key: id,
            Method: "DELETE",
            Etag: null,
            AdditionalData: null
        }
    }
}

export = deleteDocumentsCommand;