import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class captureLocalStackTracesCommand extends commandBase {

    execute(): JQueryPromise<Array<rawStackTraceResponseItem>> {
        const url = endpoints.global.threads.adminDebugThreadsStackTrace;
        return this.query<Array<rawStackTraceResponseItem>>(url, null, null, x => x.Results)
            .fail((response: JQueryXHR) => this.reportError("Unable to fetch stack traces", response.responseText, response.statusText));
    }
}

export = captureLocalStackTracesCommand;
