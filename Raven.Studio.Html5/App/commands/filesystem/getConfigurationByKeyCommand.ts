import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getConfigurationByKeyCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();        
    }

    execute(): JQueryPromise<Array<Pair<string, string[]>>> {

        var url = "/config";
        var args = {
            name: this.name
        };

        var task = $.Deferred();
        this.query<Array<Pair<string, string[]>>>(url, args, this.fs)
            .done(data => {
                var array = new Array<Pair<string, string[]>>();
                for (var key in data) {
                    if (data.hasOwnProperty(key)) {
                        var value = data[key];
                        if (value instanceof Array) {
                            array.push(new Pair(key, value));
                        }
                        else {
                            array.push(new Pair(key, new Array<string>(value)));
                        }
                    }
                }

                task.resolve(array);
            })
            .fail(response => this.reportError("Failed to retrieve filesystem configuration key. Does exist in this filesystem?", response.responseText, response.statusText));

        return task;
    }
}

export = getConfigurationByKeyCommand; 