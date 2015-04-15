import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import database = require("models/resources/database");


class getAlertsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Alerts", db);
    }

    execute(): JQueryPromise<alertContainerDto> {
        return super.execute();
    }
}

export = getAlertsCommand; 