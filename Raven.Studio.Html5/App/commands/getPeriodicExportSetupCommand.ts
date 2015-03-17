import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");


class getPeriodicExportSetupCommand extends getDocumentWithMetadataCommand {

    constructor(db: database, getGlobalConfig = false) {
        super(getGlobalConfig ? "Raven/Global/Backup/Periodic/Setup" :"Raven/Backup/Periodic/Setup", db);
    }

    execute(): JQueryPromise<periodicExportSetupDto> {
        return super.execute();
    }
}

export = getPeriodicExportSetupCommand; 