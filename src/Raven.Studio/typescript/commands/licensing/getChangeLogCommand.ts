import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getChangeLogCommand extends commandBase {

    private readonly start: number;
    private readonly perPage: number;
    
    public constructor(start: number, perPage: number) {
        super();
        
        this.start = start;
        this.perPage = perPage;
    }
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse> {
        const args = { 
            start: this.start,
            perPage: this.perPage
        }
        const url = endpoints.global.upgradeInfo.studioUpgradeInfo + this.urlEncodeArgs(args);
        
        return this.query<Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get change log", response.responseText));
    }
}

export = getChangeLogCommand;
