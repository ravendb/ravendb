import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugThreadsRunawayCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo[]> {
        const url = endpoints.global.threads.adminDebugThreadsRunaway;
        return this.query<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo[]>(url, null, null,  x => x['Runaway Threads'])
            .fail((response: JQueryXHR) => this.reportError("Failed to load Threads Runtime Info", response.responseText, response.statusText));
    }

}

export = getDebugThreadsRunawayCommand;
