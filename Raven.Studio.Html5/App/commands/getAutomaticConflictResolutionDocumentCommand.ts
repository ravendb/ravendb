import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getAutomaticConflictResolutionDocumentCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Replication/Config", db, true);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getAutomaticConflictResolutionDocumentCommand; 