import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import conflictItem = require("models/filesystem/conflictItem");

class getFilesConflictsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<conflictItem> {

        var url = "/synchronization/Conflicts";

        return this.query<filesystemListPageDto<filesystemConflictItemDto>>(url, null, this.fs)
                   .then(x => conflictItem.fromConflictItemDto(x.Items));
    }
}

export = getFilesConflictsCommand;