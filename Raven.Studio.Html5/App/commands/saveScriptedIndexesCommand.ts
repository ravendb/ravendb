import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import scriptedIndexMap = require("models/scriptedIndexMap");
import database = require("models/database");

class saveScriptedIndexesCommand extends executeBulkDocsCommand {

    constructor(private scriptedIndexes: scriptedIndexMap, db: database) {
        super(scriptedIndexes.getIndexes().map(idx => (idx.isMarkedToDelete()) ? idx.toBulkDoc("DELETE") : idx.toBulkDoc("PUT")), db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving Scripted Index...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save Scripted Index.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved Scripted Index."));
    }
}

export = saveScriptedIndexesCommand;