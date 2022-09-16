import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveTrafficWatchConfigurationCommand extends commandBase {
    
    private configuration: Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters;
    
    constructor(configuration: Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters) {
        super();
        
        this.configuration = configuration;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.trafficWatch.adminTrafficWatchConfiguration;

        return this.post<void>(url, JSON.stringify(this.configuration), null, { dataType: undefined })
            .done(() => this.reportSuccess("Traffic watch logs configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set traffic watch logs configuration", response.responseText, response.statusText));
    }
}

export = saveTrafficWatchConfigurationCommand;
