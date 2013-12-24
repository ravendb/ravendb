import commandBase = require("commands/commandBase");
import database = require("models/database");

class getAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alertDto[]> {
        return this.query("/docs/Raven/Alerts", null, this.db);
    }
}

export = getAlertsCommand; 