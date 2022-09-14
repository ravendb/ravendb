import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getTrafficWatchConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters> {
        const url = endpoints.global.trafficWatch.adminTrafficWatchConfiguration;
        
        return this.query<Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get traffic watch logs configuration`, response.responseText, response.statusText)) 
    }
}

export = getTrafficWatchConfigurationCommand;
