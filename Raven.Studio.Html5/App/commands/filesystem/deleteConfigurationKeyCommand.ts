import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class deleteConfigurationKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var url = "/config?name="+this.name;
        return this.del(url, null, this.fs);
    }
}

export = deleteConfigurationKeyCommand; 