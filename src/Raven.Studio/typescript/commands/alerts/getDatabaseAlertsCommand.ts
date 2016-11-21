import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");
import endpoints = require("endpoints");

class getDatabaseAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alert[]> {
        const url = endpoints.databases.databaseAlerts.alerts;

        return this.query<alert[]>(url, null, this.db,
            (result: Raven.Server.Alerts.Alert[]) => result.map(a => new alert(a)));
    }
}

export = getDatabaseAlertsCommand;