import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class captureLocalStackTracesCommand extends commandBase {

    execute(): JQueryPromise<stackTracesResponseDto> {
        const url = endpoints.global.threads.adminDebugThreadsStackTrace;
        return this.query<stackTracesResponseDto>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to fetch stack traces", response.responseText, response.statusText));
    }
}

export = captureLocalStackTracesCommand;
