import saveBulkOfDocuments = require("commands/saveBulkOfDocuments");
import document = require("models/document");
import database = require("models/database");
import apiKey = require("models/apiKey");

class saveApiKeysCommand extends saveBulkOfDocuments{
    constructor(apiKeys: apiKey[], db: database) {
        var bulkApiKeysArray = 
            apiKeys.map(curApiKey=> this.GetBulkDocument(curApiKey));

        super("Api Keys", bulkApiKeysArray, db);
    }

    GetBulkDocument(apiKey): bulkDocumentDto {
        return {
            AdditionalData: null,
            Document: apiKey.toDto(),
            Key: apiKey.getKey(),
            Metadata: apiKey.metadata.toDto(),
            Method: "PUT"
        };
    }
    
}

export = saveApiKeysCommand;