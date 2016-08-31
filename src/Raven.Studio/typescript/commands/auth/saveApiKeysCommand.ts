import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import apiKey = require("models/auth/apiKey");

class saveApiKeysCommand extends executeBulkDocsCommand {
    //TODO: use new dedicated endpoint
    constructor(apiKeys: apiKey[], deletedApiKeys: apiKey[]) {
        var newApiKeysBulkDocs = apiKeys.map(k => k.toBulkDoc("PUT"));
        var deletedApiKeysBulkDocs = deletedApiKeys.map(k => k.toBulkDoc("DELETE"));
        super(newApiKeysBulkDocs.concat(deletedApiKeysBulkDocs), null);
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving API keys...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save API keys.", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved API keys."));
    }
}

export = saveApiKeysCommand;
