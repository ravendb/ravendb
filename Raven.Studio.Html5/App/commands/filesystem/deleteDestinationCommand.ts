/// <reference path="../../../Scripts/typings/jquery/jquery.d.ts" />

import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");

class deleteDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: string) {
        super();
    }

    execute(): JQueryPromise<synchronizationDestinationDto[]> {

        var result = $.Deferred();

        var dtos = [];

        this.query<any>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {

                if (data && data.hasOwnProperty('destination')) {

                    // TODO: Review if the serialization method on the server is serializing this properly.
                    var value = data['destination'];
                    if (!(value instanceof Array))
                        value = [value];

                    var dtos = value.map(x => <synchronizationDestinationDto> JSON.parse(x))
                                    .filter(x => x.ServerUrl != this.destination);

                    data.destination = dtos.map(x => JSON.stringify(x));

                    var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");
                    this.put(url, JSON.stringify(data), this.fs)
                        .done(() => result.resolve(data) );
                }
            });

        return result;
    }
}

export = deleteDestinationCommand; 