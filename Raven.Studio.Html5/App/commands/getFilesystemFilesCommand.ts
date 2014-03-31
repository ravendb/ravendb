import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class getFilesystemFilesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<filesystemFileHeaderDto> {

        var url = "/stats";

        return this.query<filesystemFileHeaderDto>(url, null, this.fs);
    }
}

export = getFilesystemFilesCommand;