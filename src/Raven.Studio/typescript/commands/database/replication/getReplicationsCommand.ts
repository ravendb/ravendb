import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class getReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/DocumentReplication/Configuration", db, true);
    }

}

export = getReplicationsCommand; 
