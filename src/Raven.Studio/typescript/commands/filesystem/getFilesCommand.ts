import commandBase = require("commands/commandBase");
import file = require("models/filesystem/file");
import filesystem = require("models/filesystem/filesystem");
import queryUtil = require("common/queryUtil");
import searchResults = require("models/filesystem/searchResults");

class getFilesystemFilesCommand extends commandBase {

    constructor(private fs: filesystem, private directory: string, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<file>> {
        var filesTask = this.fetchFiles();
        var doneTask = $.Deferred<pagedResult<file>>();

        filesTask.done((results: searchResults) => {
            var files = results.Files.map(d => new file(d, true));
            var totalCount = results.FileCount;
            doneTask.resolve({ items: files, totalResultCount: totalCount });
        });
        filesTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchFiles(): JQueryPromise<searchResults> {
        var level = 1;
        if (this.directory) {
            var slashMatches = this.directory.match(/\//g).length;
            if (slashMatches) {
                level = level + slashMatches;
            }
        }

        var levelQuery = "__level:" + level;
        var args = {
            query: this.directory ? "__directoryName:" + queryUtil.escapeTerm(this.directory) + " AND " + levelQuery : levelQuery,
            start: this.skip,
            pageSize: this.take
        };

        var url = "/search";
        var task = this.query(url, args, this.fs, null);

        return task;
    }
}

export = getFilesystemFilesCommand;
