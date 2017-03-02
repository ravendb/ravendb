import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import file = require("models/filesystem/file");
import searchResults = require("models/filesystem/searchResults");

class searchByQueryCommand  extends commandBase {

    constructor(private fs: filesystem, private queryParameter: string, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<file>> {

        var filesTask = this.fetchFiles();

        var doneTask = $.Deferred<pagedResult<file>>();

        filesTask.done((results: searchResults) =>
            doneTask.resolve({ items: results.Files.map(d => new file(d, false)), totalResultCount: results.FileCount }));
        filesTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchFiles(): JQueryPromise<searchResults> {
        var args = {
            query: this.queryParameter,
            start: this.skip,
            pageSize: this.take
        };

        var url = "/search";
        return this.query(url, args, this.fs);
    }
}

export = searchByQueryCommand; 
