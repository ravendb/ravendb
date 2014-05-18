import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getConfigurationByKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();        
    }

    execute(): JQueryPromise<string> {

        var url = "/config";
        var args = {
            name: this.name
        };

        var task = $.Deferred();
        this.query<string>(url, args, this.fs)
            .done(data => {
                var prettifySpacing = 4;
                var configText = JSON.stringify(data, null, prettifySpacing);
                task.resolve(configText);
            })
            .fail(response => this.reportError("Failed to retrieve filesystem configuration key. Does exist in this filesystem?", response.responseText, response.statusText));

        return task;
    }
}

export = getConfigurationByKeyCommand; 