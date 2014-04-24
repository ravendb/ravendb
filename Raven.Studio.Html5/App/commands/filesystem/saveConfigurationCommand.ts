import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import configurationKey = require("models/filesystem/configurationKey");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private key : configurationKey, private args: any) {
        super();
    }

    execute(): JQueryPromise<any> {

        var url = "/config?name=" + encodeURIComponent(this.key.key);
        return this.put(url, JSON.stringify(this.args), this.fs);
    }
}

export = saveFilesystemConfigurationCommand; 