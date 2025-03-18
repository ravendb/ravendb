import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type RequestDto = Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters>;

class saveAdminLogsConfigurationCommand extends commandBase {
    private readonly dto: RequestDto; 

    constructor(dto: RequestDto) {
        super();
        this.dto = dto;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsConfiguration;

        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess("Admin logs configuration was successfully set"))
            .fail((response: JQueryXHR) => this.reportError("Failed to set admin logs configuration", response.responseText, response.statusText));
    }
}

export = saveAdminLogsConfigurationCommand;
