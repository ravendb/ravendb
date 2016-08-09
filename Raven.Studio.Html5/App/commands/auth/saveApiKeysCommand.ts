import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import apiKey = require("models/auth/apiKey");
import appUrl = require("common/appUrl");

class saveApiKeysCommand extends executeBulkDocsCommand {
    constructor(apiKeys: apiKey[], deletedApiKeys: apiKey[]) {
        var newApiKeysBulkDocs = apiKeys.map(k => k.toBulkDoc("PUT"));
        var deletedApiKeysBulkDocs = deletedApiKeys.map(k => k.toBulkDoc("DELETE"));
        super(newApiKeysBulkDocs.concat(deletedApiKeysBulkDocs), appUrl.getSystemDatabase());
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        this.reportInfo("Saving API keys...");

        return super.execute()
            .fail((result: JQueryXHR) => this.reportError("Unable to save API keys", result.responseText, result.statusText))
            .done(() => this.reportSuccess("Saved API keys"));
    }
}

export = saveApiKeysCommand;
