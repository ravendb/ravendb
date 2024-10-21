import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;

class replicationProgressCommand extends commandBase {

    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;
    private readonly reportFailure: boolean;

    constructor(db: database | string, location: databaseLocationSpecifier, reportFailure = true) {
        super();
        this.reportFailure = reportFailure;
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<resultsDto<ReplicationTaskProgress>> {
        const url = endpoints.databases.replication.replicationProgress;
        const args = this.location;

        return this.query<resultsDto<ReplicationTaskProgress>>(url, args, this.db)
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch replication progress`, response.responseText);    
                }
            });
    }
}

export = replicationProgressCommand; 
