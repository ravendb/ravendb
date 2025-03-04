import endpoints = require("endpoints");
import commandBase = require("commands/commandBase");

class getClusterLogEntryCommand extends commandBase {
    
    constructor(private readonly nodeTag: string, private readonly index: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry> { 
        const url = endpoints.global.rachisAdmin.adminClusterLogEntry + this.urlEncodeArgs({
            index: this.index,
            nodeTag: this.nodeTag,
        })

        return this.query<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to get cluster log entry", response.responseText, response.statusText));
    }
}

export = getClusterLogEntryCommand;
