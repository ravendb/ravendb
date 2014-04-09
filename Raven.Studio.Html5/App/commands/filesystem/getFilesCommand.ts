import commandBase = require("commands/commandBase");
import file = require("models/filesystem/file");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getFilesystemFilesCommand extends commandBase {

    constructor(private fs: filesystem, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var filesTask = this.fetchFiles();
        var totalResultsTask = this.fetchTotalResultCount();

        var doneTask = $.Deferred();
        var combinedTask = $.when(filesTask, totalResultsTask);

        combinedTask.done((filesResult: file[], resultsCount: number) => {
            doneTask.resolve(new pagedResultSet(filesResult, resultsCount))
        });
        combinedTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchFiles(): JQueryPromise<file[]> {
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var url = "/files";
        var filesSelector = (files: filesystemFileHeaderDto[]) => files.map(d => new file(d));
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