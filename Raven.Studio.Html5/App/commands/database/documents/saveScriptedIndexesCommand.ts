import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import scriptedIndex = require("models/database/index/scriptedIndex");
import database = require("models/resources/database");

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