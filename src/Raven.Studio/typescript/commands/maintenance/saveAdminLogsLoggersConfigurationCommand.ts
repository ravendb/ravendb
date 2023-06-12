import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveAdminLogsLoggersConfigurationCommand extends commandBase {
    
    private readonly configuration: dictionary<Sparrow.Logging.LogMode>;
    
    constructor(configuration: dictionary<Sparrow.Logging.LogMode>) {
        super();
        this.configuration = configuration;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLoggers;
        
        const args = {
            Configuration: {
                Loggers: this.configuration
            }
        };

        return this.post<void>(url, JSON.stringify(args), null, { dataType: undefined })
            .done(() => this.reportSuccess("Loggers configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set Loggers configuration", response.responseText, response.statusText));
    }
}

export = saveAdminLogsLoggersConfigurationCommand;
