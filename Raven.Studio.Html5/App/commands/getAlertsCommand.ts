import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import database = require("models/database");


class getAlertsCommand extends getDocumentWithMetadataCommand {

    constructor(db: database) {
        super("Raven/Alerts", db);
    }

    execute(): JQueryPromise<alertContainerDto> {
        return super.execute();
    }
}

export = getAlertsCommand; 