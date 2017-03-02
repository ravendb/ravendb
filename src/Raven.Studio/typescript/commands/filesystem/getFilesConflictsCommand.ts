import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import conflictItem = require("models/filesystem/conflictItem");

class getFilesConflictsCommand extends commandBase {

    constructor(private fs: filesystem, private skip:number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<conflictItem>> {
        var args = {
            start: this.skip,
            pageSize: this.take
        }
        var url = "/synchronization/Conflicts";

        var conflictsTask = $.Deferred<pagedResult<conflictItem>>();
        this.query<filesystemListPageDto<filesystemConflictItemDto>>(url, args, this.fs).
            fail(response => conflictsTask.reject(response)).
            done((conflicts: filesystemListPageDto<filesystemConflictItemDto>) => {
                var items = conflicts.Items.map(x => conflictItem.fromConflictItemDto(x));
                conflictsTask.resolve({ items: items, totalResultCount: conflicts.TotalCount });
            });

        return conflictsTask;
    }
}

export = getFilesConflictsCommand;
