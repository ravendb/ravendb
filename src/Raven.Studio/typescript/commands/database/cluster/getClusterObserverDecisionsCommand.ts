import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterObserverDecisionsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions> {
        
        const url = endpoints.global.rachisAdmin.adminClusterObserverDecisions;

        return this.query<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get cluster observer log entries", response.responseText));
    }
}

export = getClusterObserverDecisionsCommand;
