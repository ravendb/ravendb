import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var url = "/config";
        var args = {
            name: this.name
        }

        return this.put(url, args, this.fs);
    }
}

export = saveFilesystemConfigurationCommand; 