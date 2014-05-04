import appUrl = require("common/appUrl");
import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");

class saveFilesystemConfigurationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: synchronizationDestination) {
        super();
    }

    execute(): JQueryPromise<synchronizationDestinationDto[]> {

        var result = $.Deferred();

        var dtos = [];

        this.query<synchronizationDestinationDto[]>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {
                dtos = dtos.concat(this.destination);
                if (data && data.hasOwnProperty('destination')) {      

                    var value = data['destination'];
                    if (!(value instanceof Array))
                        value = [value];
                                                                                         
                    dtos = dtos.concat(value.map(x => <synchronizationDestinationDto> JSON.parse(x)));                    
                }
            })
            .always(x => {
                var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");       

                var doc = {
                    destination: dtos.map(y => JSON.stringify(y))
                };

                var docAsString = JSON.stringify(doc);

                this.put(url, docAsString, this.fs)
                    .done(y => {
                        result.resolve(dtos);
                    });
            });

        return result;
    }
}

export = saveFilesystemConfigurationCommand;