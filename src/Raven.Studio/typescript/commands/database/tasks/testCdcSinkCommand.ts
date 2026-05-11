import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type TestCdcSinkScript = Raven.Server.Documents.CdcSink.Test.TestCdcSinkScript;
type TestCdcSinkScriptResult = Raven.Server.Documents.CdcSink.Test.TestCdcSinkScriptResult;

class testCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: TestCdcSinkScript
    ) {
        super();
    }

    execute(): JQueryPromise<TestCdcSinkScriptResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkTest;

        return this.post<TestCdcSinkScriptResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(`Failed to test CDC Sink`, response.responseText, response.statusText);
            }
        );
    }
}

export = testCdcSinkCommand;
