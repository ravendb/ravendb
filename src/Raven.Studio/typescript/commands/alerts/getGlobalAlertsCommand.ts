import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");
import endpoints = require("endpoints");

class getGlobalAlertsCommand extends commandBase {

    execute(): JQueryPromise<alert[]> {
        const url = endpoints.global.globalAlerts.alerts;

        return this.query<alert[]>(url, null, null, (result: Raven.Server.Alerts.Alert[]) =>
            result.map(a => new alert(a)));
    }
}

export = getGlobalAlertsCommand;
