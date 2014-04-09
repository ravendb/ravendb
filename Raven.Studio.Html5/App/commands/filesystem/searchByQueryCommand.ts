import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");
import file = require("models/filesystem/file");

class searchByQueryCommand  extends commandBase {

    constructor(private fs: filesystem, private queryParameter: string, private skip: number, private take: number, private sortParameter: string) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {

        var filesTask = this.fetchFiles();
        var totalResultsTask = this.fetchTotalResultCount();

        var doneTask = $.Deferred();
        var combinedTask = $.when(filesTask, totalResultsTask);

        combinedTask.done((filesResult: filesystemFileHeaderDto[], resultsCount: number) => doneTask.resolve(new pagedResultSet(filesResult, resultsCount)));
        combinedTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchFiles(): JQueryPromise<any[]> {
        var args = {
            query: this.queryParameter,
            start: this.skip,
            pageSize: this.take
        };

        var url = "/search";
        //var filesSelector = (files: filesystemFileHeaderDto[]) => files.map(d => new file(d));
        //return this.query(url, args, this.fs, filesSelector);
        return this.query(url, args, this.fs);
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        var url = "/stats";
        var countSelector = (dto: filesystemStatisticsDto) => dto.FileCount;
        return this.query<number>(url, null, this.fs, countSelector);
    }
}

export = searchByQueryCommand; 