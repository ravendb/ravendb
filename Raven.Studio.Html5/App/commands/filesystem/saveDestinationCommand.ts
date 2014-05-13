import appUrl = require("common/appUrl");
import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");

class saveDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: synchronizationDestination) {
        super();
    }

    execute(): JQueryPromise<synchronizationDestinationDto[]> {

        var result = $.Deferred();

        var dtos = [].concat(this.destination);

        this.query<synchronizationDestinationDto[]>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {
                
                if (data && data.hasOwnProperty('Destinations')) {      

                    var value = data['Destinations'];
                    if (!(value instanceof Array))
                        value = [value];
                                                                                         
                    dtos = dtos.concat(value.map(x => <synchronizationDestinationDto> x));                    
                }
            })
            .always(x => {
                var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");       

                var doc = {
                    Destinations: dtos
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

export = saveDestinationCommand;