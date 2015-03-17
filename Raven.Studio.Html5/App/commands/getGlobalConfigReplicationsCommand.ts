import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getGlobalConfigReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Global/Replication/Destinations", db);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getGlobalConfigReplicationsCommand; 