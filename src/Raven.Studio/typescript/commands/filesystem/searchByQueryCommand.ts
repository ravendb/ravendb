import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");
import file = require("models/filesystem/file");
import searchResults = require("models/filesystem/searchResults");

class searchByQueryCommand  extends commandBase {

    constructor(private fs: filesystem, private queryParameter: string, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<any>> {

        var filesTask = this.fetchFiles();

        var doneTask = $.Deferred();

        filesTask.done((results: searchResults) => doneTask.resolve(new pagedResultSet(results.Files.map(d => new file(d, false)), results.FileCount)));
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
