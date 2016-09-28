import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");
import endpoints = require("endpoints");

type alertsResponse = {
    Global: Raven.Server.Documents.Alert[];
    Local: Raven.Server.Documents.Alert[];
}

class getOperationAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alert[]> {
        const url = endpoints.databases.operationAlerts.operationAlerts;

        const mapper = (result: alertsResponse) => {
            const globalAlerts = result.Global.map(a => new alert(a));
            globalAlerts.forEach(x => x.global = true);

            const localAlerts = result.Local.map(a => new alert(a));

            return globalAlerts.concat(localAlerts);
        }
        
        return this.query<alert[]>(url, null, this.db, mapper);
    }
}

export = getOperationAlertsCommand;
