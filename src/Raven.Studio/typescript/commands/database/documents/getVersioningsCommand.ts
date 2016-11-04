import database = require("models/resources/database");

import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class getVersioningsCommand extends getDocumentWithMetadataCommand {
    constructor(db: database) {
        super("Raven/Versioning/Configuration", db, true);
    }

}

export = getVersioningsCommand;
