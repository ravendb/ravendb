import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;

class internalReplicationProgressCommand extends commandBase {

    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;
    private readonly reportFailure: boolean;

    constructor(db: database | string, location: databaseLocationSpecifier, reportFailure = true) {
        super();
        this.reportFailure = reportFailure;
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<resultsDto<InternalReplicationTaskProgress>> {
        const url = endpoints.databases.replication.replicationInternalOutgoingProgress;
        const args = this.location;

        return this.query<resultsDto<InternalReplicationTaskProgress>>(url, args, this.db, null, { timeout: commandBase.perNodeRequestTimeoutMs })
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch internal replication progress`, response.responseText);    
                }
            });
    }
}

export = internalReplicationProgressCommand; 
