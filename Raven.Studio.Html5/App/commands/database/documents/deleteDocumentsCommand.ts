import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import database = require("models/resources/database");

class deleteDocumentsCommand extends executeBulkDocsCommand {

    constructor(docIds: Array<string>, db: database) {
        var bulkDocs = docIds.map(id => deleteDocumentsCommand.createDeleteDocument(id));
        super(bulkDocs, db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {        
        var docCount = this.docs.length;
        var docsDescription = docCount === 1 ? this.docs[0].Key : docCount + " docs";
        var alertInfoTitle = "Deleting " + docsDescription + "...";
        this.reportInfo(alertInfoTitle);

        var deleteTask = super.execute();

        deleteTask.done(() => this.reportSuccess("Deleted " + docsDescription));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + docsDescription, response.responseText, response.statusText));

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