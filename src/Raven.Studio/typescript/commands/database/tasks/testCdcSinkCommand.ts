import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import Test = Raven.Client.Documents.Operations.CdcSink.Test;

class testCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: Test.TestCdcSinkMappingRequest
    ) {
        super();
    }

    execute(): JQueryPromise<Test.TestCdcSinkMappingResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkTest;

        return this.post<Test.TestCdcSinkMappingResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(`Failed to test CDC Sink`, response.responseText, response.statusText);
            }
        );
    }
}

export = testCdcSinkCommand;
