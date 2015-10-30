import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");


class getPeriodicExportSetupCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Backup/Periodic/Setup", db);
    }

    execute(): JQueryPromise<periodicExportSetupDto> {
        return super.execute();
    }
}

export = getPeriodicExportSetupCommand; 
