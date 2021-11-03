import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getStackTraceForThreadCommand extends commandBase {

    constructor(private threadId: number) {
        super();
    }
    
    execute(): JQueryPromise<threadStackTraceResponseDto> {
        const args = {
            threadId: this.threadId
        };
        
        const url = endpoints.global.threads.adminDebugThreadsStackTrace + this.urlEncodeArgs(args);
        
        return this.query<threadStackTraceResponseDto>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to fetch stack trace for thread: " + this.threadId, response.responseText, response.statusText));
    }
}

export = getStackTraceForThreadCommand;
