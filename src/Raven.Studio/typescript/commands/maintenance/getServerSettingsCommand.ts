import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerSettingsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Config.SettingsResult> {
        const url = endpoints.global.adminConfiguration.adminConfigurationSettings;

        return this.query<Raven.Server.Config.SettingsResult>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to load the Server Settings", response.responseText, response.statusText));
    }
}

export = getServerSettingsCommand;
