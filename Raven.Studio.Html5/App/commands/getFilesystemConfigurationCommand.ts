import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class getFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<string[]> {

        var url = "/config";

        return this.query<string[]>(url, null, this.fs);
    }
}

export = getFilesystemConfigurationCommand;