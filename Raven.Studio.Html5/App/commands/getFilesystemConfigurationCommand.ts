import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class getFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {

        var url = "/config";
        var args = {
            name: this.name
        }

        return this.query<string[]>(url, args, this.fs);
    }
}

export = getFilesystemConfigurationCommand;