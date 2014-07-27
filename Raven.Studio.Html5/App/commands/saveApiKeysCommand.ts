import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import apiKey = require("models/apiKey");
import database = require("models/database");

class saveApiKeysCommand extends executeBulkDocsCommand {
    constructor(apiKeys: apiKey[], deletedApiKeys: apiKey[], db: database) {
        var x = apiKeys.map(k => k.toBulkDoc("PUT"));
        var y = deletedApiKeys.map(k => k.toBulkDoc("DELETE"));
        super(apiKeys.map(k => k.toBulkDoc("PUT")).concat(deletedApiKeys.map(k => k.toBulkDoc("DELETE"))), db);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving API keys...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save API keys.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved API keys."));
    }
}

export = saveApiKeysCommand;