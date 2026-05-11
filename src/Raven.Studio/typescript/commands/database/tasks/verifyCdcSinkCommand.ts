import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type CdcSinkVerificationResult = Raven.Server.Documents.CdcSink.CdcSinkVerificationResult;

// TODO Raven.Server.Documents.CdcSink.Handlers.CdcSinkVerifyRequest
interface VerifyCdcSinkPayload {
    ConnectionStringName: string;
    TableNames?: string[];
}

class verifyCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: VerifyCdcSinkPayload
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

export = verifyCdcSinkCommand;
