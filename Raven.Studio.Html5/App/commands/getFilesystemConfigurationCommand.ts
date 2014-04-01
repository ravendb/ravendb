import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class getFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<string[]> {

        var url = "/config";

        return this.query<string[]>(url, null, this.fs)
                   .fail(response => this.reportError("Failed to retrieve filesystem configuration.", response.responseText, response.statusText));
    }
}

export = getFilesystemConfigurationCommand;