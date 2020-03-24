import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideBackupConfigCommand extends commandBase {

    execute(): JQueryPromise<periodicBackupServerLimitsResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWide;

        return this.query<periodicBackupServerLimitsResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get server-wide configuration", response.responseText, response.statusText));
    }
}

export = getServerWideBackupConfigCommand;
