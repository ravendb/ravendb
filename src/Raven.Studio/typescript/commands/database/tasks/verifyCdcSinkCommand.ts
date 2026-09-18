import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import CdcTestRequest = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestRequest;
import CdcTestResult = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestResult;

export default class verifyCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: CdcTestRequest
    ) {
        super();
    }

    execute(): JQueryPromise<CdcTestResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkDryRun;

        return this.post<CdcTestResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError("Failed to verify CDC Sink tables", response.responseText, response.statusText);
            }
        );
    }
}
