import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private name: string, private content: any) {
        super();
    }

    execute(): JQueryPromise<any> {

        var url = "/config";
        var args = {
            name: this.name
        };

        // TODO: Send the content to the server.
        return this.put(url, args, this.fs);
    }
}

export = saveFilesystemConfigurationCommand; 