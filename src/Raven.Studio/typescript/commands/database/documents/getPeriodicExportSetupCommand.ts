import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import database = require("models/resources/database");


class getPeriodicExportSetupCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/PeriodicExport/Configuration", db, true);
    }

    execute(): JQueryPromise<Raven.Server.Documents.PeriodicExport.PeriodicExportConfiguration> {
        return super.execute();
    }
}

export = getPeriodicExportSetupCommand; 
