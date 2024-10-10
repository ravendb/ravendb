import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterLogCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Rachis.RaftDebugView> { 
        const url = endpoints.global.rachisAdmin.adminClusterLog + this.urlEncodeArgs({
            pageSize: 1024 * 1024
        });

        return this.query<Raven.Server.Rachis.RaftDebugView>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to get cluster log", response.responseText, response.statusText));
    }
}

export = getClusterLogCommand;
