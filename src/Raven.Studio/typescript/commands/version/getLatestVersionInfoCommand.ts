import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type VersionInfoDto = Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo;

class getLatestVersionInfoCommand extends commandBase {
    constructor(private refresh?: boolean) {
        super();
    }
 
    execute(): JQueryPromise<VersionInfoDto> {
        const args = { refresh: this.refresh };
        const url = endpoints.global.buildVersion.buildVersionUpdates 
            + (this.refresh ? this.urlEncodeArgs(args) : '');
        return this.post<VersionInfoDto>(url, null)
            .fail(response => {
                this.reportError(`Failed to get latest version info.`,
                    response.responseText,
                    response.statusText);
            });
    }
}

export = getLatestVersionInfoCommand; 
