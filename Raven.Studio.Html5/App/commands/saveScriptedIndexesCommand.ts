import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import scriptedIndex = require("models/scriptedIndex");
import database = require("models/database");

class saveScriptedIndexesCommand extends executeBulkDocsCommand {

    constructor(private scriptedIndexes: Array<scriptedIndex>, db: database) {
        super(scriptedIndexes.map(idx => (idx.isMarkedToDelete()) ? idx.toBulkDoc("DELETE") : idx.toBulkDoc("PUT")), db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving Scripted Index...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save Scripted Index.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved Scripted Index."));
    }
}

export = saveScriptedIndexesCommand;
