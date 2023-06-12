import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAdminLogsLoggersConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<adminLogsLoggersConfigurationResponse> {
        const url = endpoints.global.adminLogs.adminLoggersConfiguration;
        
        return this.query<adminLogsLoggersConfigurationResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get loggers configuration", response.responseText, response.statusText)) 
    }
}

export = getAdminLogsLoggersConfigurationCommand;
