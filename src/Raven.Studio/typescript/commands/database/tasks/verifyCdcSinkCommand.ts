import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

type CdcSinkVerificationResult = Raven.Server.Documents.CdcSink.CdcSinkVerificationResult;

interface VerifyCdcSinkPayload {
    ConnectionStringName: string;
    TableNames?: string[];
}

class verifyCdcSinkCommand extends commandBase {
    constructor(private db: database | string, private payload: VerifyCdcSinkPayload) {
        super();
    }

    execute(): JQueryPromise<CdcSinkVerificationResult> {
        const url = "TODO";

        return this.post<CdcSinkVerificationResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(`Failed to verify CDC Sink source`, response.responseText, response.statusText);
            }
        );
    }
}

export = verifyCdcSinkCommand;
