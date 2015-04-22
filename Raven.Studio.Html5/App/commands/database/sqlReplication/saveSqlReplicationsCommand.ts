import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import database = require("models/resources/database");

class saveSqlReplicationsCommand extends executeBulkDocsCommand {

    constructor(onScreenReplications: sqlReplication[], deletedReplications: sqlReplication[], db: database) {
        var bulkPutReplications: bulkDocumentDto[] = onScreenReplications.map(sr => sr.toBulkDoc("PUT"));
        var bulkDeleteReplications: bulkDocumentDto[] = deletedReplications.map(sr => sr.toBulkDoc("DELETE"));
        super(bulkPutReplications.concat(bulkDeleteReplications), db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving SQL replications...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save SQL replications.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved SQL replications."));
    }
}

export = saveSqlReplicationsCommand;