import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Replication/Destinations", db, true);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getReplicationsCommand; 