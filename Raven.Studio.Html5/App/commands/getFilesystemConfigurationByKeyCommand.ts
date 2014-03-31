import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class getFilesystemConfigurationByKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();        
    }

    execute(): JQueryPromise<any> {

        var url = "/config";
        var args = {
            name: this.name
        }

        return this.query<any>(url, args, this.fs);
    }
}

export = getFilesystemConfigurationByKeyCommand; 