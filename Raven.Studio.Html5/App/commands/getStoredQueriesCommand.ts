import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getStoredQueriesCommand extends getDocumentWithMetadataCommand {
    static storedQueryDocId = "Raven/Studio/Query/Recent";

    constructor(db: database) {
        super(getStoredQueriesCommand.storedQueryDocId, db);
    }

    execute(): JQueryPromise<storedQueryContainerDto> {
        return <any>super.execute();
    }
}

export = getStoredQueriesCommand;