import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getConfigurationByKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();        
    }

    execute(): JQueryPromise<pagedResultSet> {

        var url = "/config";
        var args = {
            name: this.name
        };

        var task = $.Deferred();
        this.query<Array<Pair<string, string>>>(url, args, this.fs)
            //.done(pairs => task.resolve(new pagedResultSet(pairs, pairs.totalResults)))
            .done(pairs => {
                var pair = pairs[0];
            })
            .fail(response => this.reportError("Failed to retrieve filesystem configuration key. Does exist in this filesystem?", response.responseText, response.statusText));

        return task;
    }
}

export = getConfigurationByKeyCommand; 