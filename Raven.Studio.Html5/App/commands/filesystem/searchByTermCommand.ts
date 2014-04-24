import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class searchFilesystemByTermCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var url = "/search/Terms";
        return this.query<string[]>(url, null, this.fs);
    }
}

export = searchFilesystemByTermCommand; 