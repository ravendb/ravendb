import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class getSqlReplicationConnectionsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/SqlReplication/Connections", db);
    }

    execute(): JQueryPromise<Raven.Server.Documents.SqlReplication.SqlConnections> {
        return super.execute();
    }
}

export = getSqlReplicationConnectionsCommand;
