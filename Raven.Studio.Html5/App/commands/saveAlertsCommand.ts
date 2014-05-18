import saveDocumentCommand = require("commands/saveDocumentCommand");
import document = require("models/document");
import database = require("models/database");

class saveAlertsCommand extends saveDocumentCommand {
    constructor(alertDoc: alertContainerDto, db: database) {
        var doc = new document(alertDoc);
        super("Raven/Alerts", doc, db);
    }
}

export = saveAlertsCommand;