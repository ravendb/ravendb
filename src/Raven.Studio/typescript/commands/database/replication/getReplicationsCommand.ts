import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class getReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super(documentReplicationConfiguration, db, true);
    }

}

export = getReplicationsCommand; 
