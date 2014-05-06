import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFilesystemStatsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<filesystemStatisticsDto> {
        var url = "/stats";
        return this.query<filesystemStatisticsDto>(url, null, this.fs);
    }
}

export = getFilesystemStatsCommand;