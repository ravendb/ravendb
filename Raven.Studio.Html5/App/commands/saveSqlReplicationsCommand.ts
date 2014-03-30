import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import sqlReplication = require("models/sqlReplication");
import database = require("models/database");

class saveSqlReplicationsCommand extends executeBulkDocsCommand {

    constructor(sqlReplications: sqlReplication[], db: database) {
        super(sqlReplications.map(sr => sr.toBulkDoc("PUT")), db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving SQL replications...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save SQL replications.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved SQL replications."));
    }
}

export = saveSqlReplicationsCommand;