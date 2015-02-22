import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getAutomaticConflictResolutionDocumentCommand extends getDocumentWithMetadataCommand {

    constructor(db: database, global: boolean  = false) {
        super(global ? "Raven/Global/Replication/Config":"Raven/Replication/Config", db);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getAutomaticConflictResolutionDocumentCommand; 