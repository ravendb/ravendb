import commandBase = require("commands/commandBase");
import file = require("models/file");
import filesystem = require("models/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getFilesystemFilesCommand extends commandBase {

    constructor(private fs: filesystem, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var url = "/files";
        var filesSelector = (files: filesystemFileHeaderDto[]) => files.map(d => new file(d));
        var doneTask = $.Deferred();
        this.query(url, args, this.fs, filesSelector).done(collection => {
            var resultSet = new pagedResultSet(collection, collection.count());
            doneTask.resolve(resultSet);
        });

        return doneTask;
    }
}

export = getFilesystemFilesCommand;