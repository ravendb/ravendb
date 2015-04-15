import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import database = require("models/resources/database");

class getGlobalConfigReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Global/Replication/Destinations", db);
    }

    execute(): JQueryPromise<replicationsDto> {
        return super.execute();
    }
}

export = getGlobalConfigReplicationsCommand; 