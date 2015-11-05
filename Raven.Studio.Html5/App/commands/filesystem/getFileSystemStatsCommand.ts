import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFileSystemStatsCommand extends commandBase {

    constructor(private fs: filesystem, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<filesystemStatisticsDto> {
        var url = "/stats";
        return this.query<filesystemStatisticsDto>(url, null, this.fs, null, this.getTimeToAlert(this.longWait));
    }
}

export = getFileSystemStatsCommand;
