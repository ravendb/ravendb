import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterLogCommand extends commandBase {

    private readonly nodeTag: string | undefined;
    private readonly from: number | undefined;
    private readonly pageSize: number;
    
    constructor(nodeTag: string | undefined, from: number, pageSize: number) {
        super();
        
        this.nodeTag = nodeTag;
        this.from = from;
        this.pageSize = pageSize;
    }
    
    execute(): JQueryPromise<Raven.Server.Rachis.RaftDebugView> { 
        const url = endpoints.global.rachisAdmin.adminClusterLog + this.urlEncodeArgs({
            pageSize: this.pageSize,
            from: this.from ?? undefined,
            nodeTag: this.nodeTag,
        });

        return this.query<Raven.Server.Rachis.RaftDebugView>(url, null);
    }
}

export = getClusterLogCommand;
