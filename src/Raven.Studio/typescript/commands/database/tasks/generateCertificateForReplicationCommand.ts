import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class generateCertificateForReplicationCommand extends commandBase {

    constructor(private db: database, private expirationInMonths: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate> {
        const args = {
            validMonths: this.expirationInMonths
        };
        
        const url = endpoints.databases.pullReplication.adminPullReplicationGenerateCertificate + this.urlEncodeArgs(args);

        return this.post<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate>(url, null, this.db, undefined, 20000)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to generate a certificate for replication", response.responseText, response.statusText);
            });
    }
}

export = generateCertificateForReplicationCommand;
