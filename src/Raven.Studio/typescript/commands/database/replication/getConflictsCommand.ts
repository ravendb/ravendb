import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConflictsCommand extends commandBase {

    constructor(private ownerDb: database, private start: number, private pageSize: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<replicationConflictListItemDto>> {
        const url = endpoints.databases.replication.replicationConflicts;

        const transformer = (result: resultsWithTotalCountDto<replicationConflictListItemDto>) => {
            return {
                items: result.Results,
                totalResultCount: result.TotalResults
            } as pagedResult<replicationConflictListItemDto>;
        }

        return this.query<pagedResult<replicationConflictListItemDto>>(url, null, this.ownerDb, transformer);
    }
}

export = getConflictsCommand;
