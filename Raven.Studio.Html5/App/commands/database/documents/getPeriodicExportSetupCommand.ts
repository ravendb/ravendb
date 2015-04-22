import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import database = require("models/resources/database");


class getPeriodicExportSetupCommand extends getDocumentWithMetadataCommand {

    constructor(db: database, getGlobalConfig = false) {
        super(getGlobalConfig ? "Raven/Global/Backup/Periodic/Setup" :"Raven/Backup/Periodic/Setup", db);
    }

    execute(): JQueryPromise<periodicExportSetupDto> {
        return super.execute();
    }
}

export = getPeriodicExportSetupCommand; 