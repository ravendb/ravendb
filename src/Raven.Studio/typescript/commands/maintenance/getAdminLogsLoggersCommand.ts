import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAdminLogsLoggersCommand extends commandBase {
    
    execute(): JQueryPromise<adminLogsLoggersResponse> {
        const url = endpoints.global.adminLogs.adminLogsLoggers;
        
        return this.query<adminLogsLoggersResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get loggers", response.responseText, response.statusText)) 
    }
}

export = getAdminLogsLoggersCommand;
