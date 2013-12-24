import commandBase = require("commands/commandBase");
import database = require("models/database");
import alert = require("models/alert");

class getAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alert[]> {
        var selector = (alertContainer: alertContainerDto) => alertContainer.Alerts.map(d => new alert(d));
        return this.query("/docs/Raven/Alerts", null, this.db, selector);
    }
}

export = getAlertsCommand; 