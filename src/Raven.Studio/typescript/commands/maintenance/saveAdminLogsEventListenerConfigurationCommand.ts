import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type EventListenerConfigurationDto = Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration;

class saveAdminLogsEventListenerConfigurationCommand extends commandBase {
    private readonly config: EventListenerConfigurationDto;

    constructor(config: EventListenerConfigurationDto) {
        super();
        this.config = config;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminEventListenerConfiguration;

        return this.post<void>(url, JSON.stringify(this.config), null, { dataType: undefined })
            .done(() => this.reportSuccess("Event Listener configuration was successfully set"))
            .fail((response: JQueryXHR) =>
                this.reportError(
                    "Failed to set Event Listener configuration",
                    response.responseText,
                    response.statusText
                )
            );
    }
}

export = saveAdminLogsEventListenerConfigurationCommand;
