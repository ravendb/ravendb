import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import database = require("models/resources/database");

class deleteDocumentsCommand extends executeBulkDocsCommand {

    constructor(docIds: Array<string>, db: database) {
        const bulkDocs = docIds.map(id => deleteDocumentsCommand.createDeleteDocument(id));
        super(bulkDocs, db);
    }

    private static createDeleteDocument(id: string): Partial<Raven.Server.Documents.Handlers.Batches.BatchRequestParser.CommandData> {
        return {
            Id: id,
            Type: "DELETE",
            ChangeVector: null
        };
    }
}

export = deleteDocumentsCommand;
