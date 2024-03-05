import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getChangeLogCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse> {
        const url = endpoints.global.upgradeInfo.studioUpgradeInfo;
        
        return this.query<Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get change log", response.responseText));
    }
}

export = getChangeLogCommand;
