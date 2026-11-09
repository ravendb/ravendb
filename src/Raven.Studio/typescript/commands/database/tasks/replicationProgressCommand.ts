import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;

class replicationProgressCommand extends commandBase {

    constructor(private readonly db: database | string, private readonly location: databaseLocationSpecifier, private readonly reportFailure = true, private readonly taskName: string = null, private readonly exact = false) {
        super();
    }

    execute(): JQueryPromise<resultsDto<ReplicationTaskProgress>> {
        const args = {
            ...this.location,
            name: this.taskName ?? undefined,
            exact: this.exact ? true : undefined,
        };
        const url = endpoints.databases.replication.replicationProgress + this.urlEncodeArgs(args);

        return this.query<resultsDto<ReplicationTaskProgress>>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch replication progress`, response.responseText);    
                }
            });
    }
}

export = replicationProgressCommand; 
