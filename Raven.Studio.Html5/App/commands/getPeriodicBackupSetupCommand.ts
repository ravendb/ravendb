import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");


class getPeriodicBackupSetupCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Backup/Periodic/Setup", db);
    }

    execute(): JQueryPromise<periodicBackupSetupDto> {
        return super.execute();
    }
}

export = getPeriodicBackupSetupCommand; 