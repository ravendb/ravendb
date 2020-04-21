import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import adminLogsOnDiskConfig = require("models/database/debug/adminLogsOnDiskConfig");

class saveAdminLogsConfigurationCommand extends commandBase {
    
    constructor(private logsConfiguration: adminLogsOnDiskConfig) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsConfiguration;
        const payload = {
            Mode: this.logsConfiguration.selectedLogMode(),
            RetentionTime: this.logsConfiguration.retentionTime(),
            RetentionSize: this.logsConfiguration.retentionSize(),
            Compress: this.logsConfiguration.compress
        };

        return this.post<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("Admin logs configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set admin logs configuration", response.responseText, response.statusText));
    }
}

export = saveAdminLogsConfigurationCommand;
