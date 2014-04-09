/// <reference path="../../../Scripts/typings/jquery/jquery.d.ts" />

import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class deleteDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        var result = $.Deferred();

        this.query<any>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {
                if (data) {

                    // TODO: Review if the serialization method on the server is serializing this properly.
                    if (!(data.url instanceof Array))
                        data.url = [data.url];

                    data.url = data.url.filter(x => x != this.destination);

                    var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");
                    this.put(url, JSON.stringify(data), this.fs)
                        .done(() => result.resolve(data) );
                }
            });

        return result;
    }
}

export = deleteDestinationCommand; 