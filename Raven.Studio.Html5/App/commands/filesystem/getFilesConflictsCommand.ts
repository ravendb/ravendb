import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import conflictItem = require("models/filesystem/conflictItem");
import pagedResultSet = require("common/pagedResultSet");

class getFilesConflictsCommand extends commandBase {

    constructor(private fs: filesystem, private skip:number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            start: this.skip,
            pageSize: this.take
        }
        var url = "/synchronization/Conflicts";

        var conflictsTask = $.Deferred();
        this.query<filesystemListPageDto<filesystemConflictItemDto>>(url, args, this.fs).
            fail(response => conflictsTask.reject(response)).
            done((conflicts: filesystemListPageDto<filesystemConflictItemDto>) => {
                var items = conflicts.Items.map(x => conflictItem.fromConflictItemDto(x));
                var resultsSet = new pagedResultSet(items, conflicts.TotalCount);
                conflictsTask.resolve(resultsSet);
            });

        return conflictsTask;
    }
}

export = getFilesConflictsCommand;
