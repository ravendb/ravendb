import commandBase = require("commands/commandBase");
import file = require("models/filesystem/file");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getFilesystemFilesCommand extends commandBase {

    constructor(private fs: filesystem, private directory: string, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var filesTask = this.fetchFiles();
        var totalResultsTask = this.fetchTotalResultCount();

        var doneTask = $.Deferred();
        var combinedTask = $.when(filesTask, totalResultsTask);

        combinedTask.done((filesResult: file[], resultsCount: number) => {
            doneTask.resolve(new pagedResultSet(filesResult, resultsCount));
        });
        combinedTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchFiles(): JQueryPromise<file[]> {
        var level = 2;
        if (this.directory) {
            var slashMatches = new Array(this.directory).count(x => x === "/");
            if (slashMatches) {
                level = level + slashMatches;
            }
        }
        var args = {
            query: this.directory? "__directory:/"+this.directory+" AND __level:"+level: null,
            start: this.skip,
            pageSize: this.take
        };

        var url = "/search";
        var filesSelector = (results: searchResults) => results.Files.map(d => new file(d, true));
        var task = this.query(url, args, this.fs, filesSelector);

        return task;
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        var url = "/stats";
        var countSelector = (dto: filesystemStatisticsDto) => dto.FileCount;
        return this.query<number>(url, null, this.fs, countSelector);
    }
}

export = getFilesystemFilesCommand;