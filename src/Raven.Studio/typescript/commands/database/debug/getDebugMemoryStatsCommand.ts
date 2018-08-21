import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugMemoryStatsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfo> {
        const url = endpoints.global.memoryDebug.adminDebugMemoryStats;
        return this.query<Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfo>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to load memory statistics", response.responseText, response.statusText));
    }

}

export = getDebugMemoryStatsCommand;
