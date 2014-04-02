import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFilesystemSynchronizationStatusCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<filesystemSynchronizationReportDto> {
        var url = "/synchronization/Status";
        return this.query<filesystemSynchronizationReportDto>(url, null, this.fs);
    }
}

export = getFilesystemSynchronizationStatusCommand;