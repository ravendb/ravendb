import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import database = require("models/resources/database");

class saveSqlReplicationsCommand extends executeBulkDocsCommand {

    constructor(onScreenReplications: sqlReplication[], deletedReplications: sqlReplication[], db: database) {
        var bulkPutReplications: Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[] = onScreenReplications.map(sr => sr.toBulkDoc("PUT"));
        var bulkDeleteReplications: Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[] = deletedReplications.map(sr => sr.toBulkDoc("DELETE"));
        super(bulkPutReplications.concat(bulkDeleteReplications), db);
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[]> {
        this.reportInfo("Saving SQL replications...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save SQL replications.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved SQL replications."));
    }
}

export = saveSqlReplicationsCommand;
