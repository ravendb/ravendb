import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class removeEntryFromLogCommand extends commandBase {
    
    private readonly index: number;
    
    constructor(index: number) {
        super();
        this.index = index;
    }

    execute(): JQueryPromise<void> { 
        const url = endpoints.global.rachisAdmin.adminClusterRemoveEntryFromLog + this.urlEncodeArgs({
            index: this.index
        });

        return this.post<void>(url, null)
            .done(() => this.reportSuccess("Cluster Log entry was removed"))
            .fail((response: JQueryXHR) => this.reportError("Unable to remove entry log from cluster", response.responseText, response.statusText));
    }
}

export = removeEntryFromLogCommand;
