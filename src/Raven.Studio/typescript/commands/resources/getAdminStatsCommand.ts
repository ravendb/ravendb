import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import ServerStatistics = Raven.Server.ServerWide.ServerStatistics;

class getAdminStatsCommand extends commandBase {

    private readonly nodeTag: string;
    
    constructor(nodeTag: string) {
        super();
        this.nodeTag = nodeTag;
    }
    
    execute(): JQueryPromise<ServerStatistics> {
        const url = endpoints.global.adminStats.adminStats;
        const args = {
            nodeTag: this.nodeTag,
        }

        return this.query<ServerStatistics>(url, args, undefined)
            .fail((response: JQueryXHR) => this.reportError("Failed to load server statistics", response.responseText, response.statusText));
    }
}

export = getAdminStatsCommand;
