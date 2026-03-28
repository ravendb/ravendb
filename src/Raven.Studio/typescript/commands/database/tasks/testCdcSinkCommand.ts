import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testCdcSinkCommand extends commandBase {
    constructor(
        private db: database | string,
        private payload: Raven.Server.Documents.CdcSink.Test.TestCdcSinkScript
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.CdcSink.Test.TestCdcSinkScriptResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkTest;

        return this.post<Raven.Server.Documents.CdcSink.Test.TestCdcSinkScriptResult>(
            url,
            JSON.stringify(this.payload),
            this.db
        ).fail((response: JQueryXHR) => {
            this.reportError(`Failed to test CDC Sink`, response.responseText, response.statusText);
        });
    }
}

export = testCdcSinkCommand;
