import appUrl = require("common/appUrl");
import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private url: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var result = $.Deferred();

        var newValue = { url: [this.url] };


        var keys: string[] = [this.url];

        this.query<any>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(x => {
                if (x){                    
                    keys = x.url.concat(keys);
                }
            })
            .always(x => {
                var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");

                var doc = { url: keys };

                result = this.put(url, JSON.stringify(doc), this.fs)
                             .done(y => { result.resolve(newValue) })
            });

        return result;
    }
}

export = saveFilesystemConfigurationCommand;