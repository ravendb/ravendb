import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");

class getVersioningCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Versioning", db);
    }

    execute(): JQueryPromise<versioningDto> {
        return super.execute();
    }
}

export = getVersioningCommand; 