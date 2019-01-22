import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class pullReplicationGenerateCertificateCommand extends commandBase {

    constructor(private db: database, private expirationInYears: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate> {
        const args = {
            validYears: this.expirationInYears
        };
        
        const url = endpoints.databases.pullReplication.adminPullReplicationGenerateCertificate + this.urlEncodeArgs(args);

        return this.post<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate>(url, null, this.db, undefined, 20000)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to generate certificate for pull replication", response.responseText, response.statusText);
            });
    }
}

export = pullReplicationGenerateCertificateCommand;
