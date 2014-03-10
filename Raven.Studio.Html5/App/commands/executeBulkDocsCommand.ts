import commandBase = require("commands/commandBase");
import database = require("models/database");

class executeBulkDocsCommand extends commandBase {
    constructor(public docs: bulkDocumentDto[], private db: database) {
        super();
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        return this.post("/bulk_docs", JSON.stringify(this.docs), this.db);
    }
}

export = executeBulkDocsCommand; 