import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type EventListenerConfigurationDto = Omit<Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration, "Persist">;

class getAdminLogsEventListenerConfigurationCommand extends commandBase {
    execute(): JQueryPromise<EventListenerConfigurationDto> {
        const url = endpoints.global.adminLogs.adminEventListenerConfiguration;

        return this.query<EventListenerConfigurationDto>(url, null).fail((response: JQueryXHR) =>
            this.reportError(
                "Failed to get Event Listener configuration",
                response.responseText,
                response.statusText
            )
        );
    }
}

export = getAdminLogsEventListenerConfigurationCommand;
