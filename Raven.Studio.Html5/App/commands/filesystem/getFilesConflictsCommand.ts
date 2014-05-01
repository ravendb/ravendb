import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import conflictItem = require("models/filesystem/conflictItem");

class getFilesConflictsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<conflictItem[]> {

        var url = "/synchronization/Conflicts";

        var resultsSelector = (page: filesystemListPageDto<filesystemConflictItemDto>) => page.Items.map(x => conflictItem.fromConflictItemDto(x));
        return this.query(url, null, this.fs, resultsSelector);
    }
}

export = getFilesConflictsCommand;