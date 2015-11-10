import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import database = require("models/resources/database");

class getAutomaticConflictResolutionDocumentCommand extends getDocumentWithMetadataCommand {

    constructor(db: database, global: boolean  = false) {
        super(global ? "Raven/Global/Replication/Config":"Raven/Replication/Config", db);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getAutomaticConflictResolutionDocumentCommand; 
