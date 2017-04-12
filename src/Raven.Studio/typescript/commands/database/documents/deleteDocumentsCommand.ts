import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import database = require("models/resources/database");

class deleteDocumentsCommand extends executeBulkDocsCommand {

    constructor(docIds: Array<string>, db: database) {
        const bulkDocs = docIds.map(id => deleteDocumentsCommand.createDeleteDocument(id));
        super(bulkDocs, db);
    }

    private static createDeleteDocument(id: string): Raven.Server.Documents.Handlers.BatchRequestParser.CommandData {
        return {
            Key: id,
            Method: "DELETE",
            Etag: null
        } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData;
    }
}

export = deleteDocumentsCommand;
