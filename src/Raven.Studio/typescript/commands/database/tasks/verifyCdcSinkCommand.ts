import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type CdcSinkVerificationResult = Raven.Server.Documents.CdcSink.CdcSinkVerificationResult;

export default class verifyCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: Raven.Server.Documents.CdcSink.Handlers.CdcSinkVerifyRequest
    ) {
        super();
    }

    execute(): JQueryPromise<CdcSinkVerificationResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkVerify;

        return this.post<CdcSinkVerificationResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(`Failed to verify CDC Sink source`, response.responseText, response.statusText);
            }
        );
    }
}
