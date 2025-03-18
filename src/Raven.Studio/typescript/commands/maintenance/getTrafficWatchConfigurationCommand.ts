import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type TrafficWatchConfiguration = Omit<Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters, "Persist">;

class getTrafficWatchConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<TrafficWatchConfiguration> {
        const url = endpoints.global.trafficWatch.adminTrafficWatchConfiguration;
        
        return this.query<TrafficWatchConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get traffic watch logs configuration`, response.responseText, response.statusText)) 
    }
}

export = getTrafficWatchConfigurationCommand;
