import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterLogCommand extends commandBase {

    private readonly from: number | undefined;
    private readonly pageSize: number;
    
    constructor(from: number, pageSize: number) {
        super();
        
        this.from = from;
        this.pageSize = pageSize;
    }
    
    execute(): JQueryPromise<Raven.Server.Rachis.RaftDebugView> { 
        const url = endpoints.global.rachisAdmin.adminClusterLog + this.urlEncodeArgs({
            pageSize: this.pageSize,
            from: this.from ?? undefined,
        });

        return this.query<Raven.Server.Rachis.RaftDebugView>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to get cluster log", response.responseText, response.statusText));
    }
}

export = getClusterLogCommand;
