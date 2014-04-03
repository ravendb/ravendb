import appUrl = require("common/appUrl");
import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private url: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var result = $.Deferred();

        var doc = { url: [this.url] };

        var keys: string[] = [this.url];

        this.query<any>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {
                if (data) {                                                                               
                    data.url = [].concat(this.url).concat(data.url);
                    doc = data;
                }
            })
            .always(x => {
                var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");
        
                result = this.put(url, JSON.stringify(doc), this.fs)
                    .done(y => { result.resolve(doc); });
            });

        return result;
    }
}

export = saveFilesystemConfigurationCommand;