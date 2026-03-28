import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class verifyCdcSinkSourceCommand extends commandBase {
    constructor(
        private db: database | string,
        private connectionStringName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkVerify;

        const payload = {
            ConnectionStringName: this.connectionStringName
        };

        return this.post<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult>(
            url,
            JSON.stringify(payload),
            this.db
        ).fail((response: JQueryXHR) => {
            this.reportError(`Failed to verify CDC Sink source`, response.responseText, response.statusText);
        });
    }
}

export = verifyCdcSinkSourceCommand;
