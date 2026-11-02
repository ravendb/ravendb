import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;

class internalReplicationProgressCommand extends commandBase {

    constructor(private readonly db: database | string, private readonly location: databaseLocationSpecifier, private readonly reportFailure = true, private readonly exact = false) {
        super();
    }

    execute(): JQueryPromise<resultsDto<InternalReplicationTaskProgress>> {
        const args = {
            ...this.location,
            exact: this.exact ? true : undefined,
        };
        const url = endpoints.databases.replication.replicationInternalOutgoingProgress + this.urlEncodeArgs(args);

        return this.query<resultsDto<InternalReplicationTaskProgress>>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch internal replication progress`, response.responseText);    
                }
            });
    }
}

export = internalReplicationProgressCommand; 
