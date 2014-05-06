import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationReport = require("models/filesystem/synchronizationReport");

class getFilesystemSyncCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<filesystemSynchronizationReportDto> {
        var url = "/synchronization/Status";
        return this.query<filesystemSynchronizationReportDto>(url, null, this.fs, results => results.map(x => new synchronizationReport(x)));
    }
}

export = getFilesystemSyncCommand;