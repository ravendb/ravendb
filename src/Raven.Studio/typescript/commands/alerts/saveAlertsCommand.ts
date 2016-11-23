import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");

class saveAlertsCommand extends saveDocumentCommand {
    /* TODO
    constructor(alertDoc: alertContainerDto, db: database) {
        var doc = new document(alertDoc);
        super("Raven/Alerts", doc, db);
    }*/
}

export = saveAlertsCommand;
