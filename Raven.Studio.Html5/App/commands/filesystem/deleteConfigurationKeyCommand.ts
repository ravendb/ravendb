import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class deleteConfigurationKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var url = "/config";
        var args = {
            name: this.name
        };

        return this.del(url, args, this.fs);
    }
}

export = deleteConfigurationKeyCommand; 