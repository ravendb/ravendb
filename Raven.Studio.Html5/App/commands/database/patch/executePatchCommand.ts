import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import database = require("models/resources/database");

class executePatchCommand extends executeBulkDocsCommand {

    constructor(bulkDocsDto: bulkDocumentDto[], db: database, private test: boolean) {
        super(bulkDocsDto, db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {

        var failInfo = (this.test) ? "Patch test failed." : "Unable to patch documents.";
        var doneInfo = (this.test) ? "Patch tested. See the results below." : "Documents patched.";

        if (!this.test) {
            this.reportInfo("Patching documents...");
        }

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError(failInfo, result.responseText, result.statusText))
            .done(() => this.reportSuccess(doneInfo));
    }
}

export = executePatchCommand;