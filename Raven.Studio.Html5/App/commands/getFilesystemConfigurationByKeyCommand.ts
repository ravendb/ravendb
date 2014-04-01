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
        };

        return this.query<any>(url, args, this.fs)
                   .fail(response => this.reportError("Failed to retrieve filesystem configuration key. Does exist in this filesystem?", response.responseText, response.statusText));
    }
}

export = getFilesystemConfigurationByKeyCommand; 