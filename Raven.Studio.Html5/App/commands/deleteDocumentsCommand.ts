import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import database = require("models/database");

class deleteDocumentsCommand extends executeBulkDocsCommand {

    constructor(docIds: Array<string>, db: database) {
        var bulkDocs = docIds.map(id => deleteDocumentsCommand.createDeleteDocument(id));
        super(bulkDocs, db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {        
        var docCount = this.docs.length;
        var alertInfoTitle = docCount === 1 ? "Deleting " + this.docs[0].Key : "Deleting " + docCount + " docs...";
        this.reportInfo(alertInfoTitle);

        var deleteTask = super.execute();

        deleteTask.done(() => this.reportSuccess("Deleted " + docCount + " docs"));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete docs", response.responseText, response.statusText));

        return deleteTask;
    }

    private static createDeleteDocument(id: string): bulkDocumentDto {
        return {
            Key: id,
            Method: "DELETE",
            Etag: null,
            AdditionalData: null
        };
    }
}

export = deleteDocumentsCommand;