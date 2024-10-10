import endpoints = require("endpoints");
import commandBase from "commands/commandBase";

class getClusterLogEntryCommand extends commandBase {
    
    private readonly index: number;

    constructor(index: number) {
        super();
        this.index = index;
    }

    execute(): JQueryPromise<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry> { 
        const url = endpoints.global.rachisAdmin.adminClusterLogEntry + this.urlEncodeArgs({
            index: this.index
        })

        return this.query<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to get cluster log entry", response.responseText, response.statusText));
    }
}

export = getClusterLogEntryCommand;
