import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import configurationKey = require("models/filesystem/configurationKey");

class getConfigurationCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<configurationKey[]> {

        var url = "/config";

        return this.query<configurationKey[]>(url, null, this.fs, results => results.map( x => new configurationKey(this.fs, x)))
                   .fail(response => this.reportError("Failed to retrieve filesystem configuration.", response.responseText, response.statusText));
    }
}

export = getConfigurationCommand;