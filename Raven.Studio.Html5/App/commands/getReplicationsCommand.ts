import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getReplicationsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Replication/Destinations", db);
    }

    execute(): JQueryPromise<replicationsDto> {
        var getTask = super.execute();
        //getTask.fail((response: JQueryXHR) => this.reportError("Failed to get replications!", response.responseText, response.statusText));
        return getTask;
    }
}

export = getReplicationsCommand; 
