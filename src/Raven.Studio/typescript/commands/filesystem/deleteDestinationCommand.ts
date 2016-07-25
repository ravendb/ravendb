import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class deleteDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: synchronizationDestinationDto) {
        super();
    }

    execute(): JQueryPromise<synchronizationDestinationDto[]> {

        var result = $.Deferred();

        var serverUrl = this.destination.ServerUrl;
        var fileSystem = this.destination.FileSystem;

        this.query<any>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
            .done(data => {

                if (data && data.hasOwnProperty('Destinations')) {

                    var destinationsRaw = data['Destinations'];
                    var destinations: synchronizationDestinationDto[] =
                        (destinationsRaw instanceof Array) ? destinationsRaw : [destinationsRaw];

                    var dtos = destinations.filter(x => x.ServerUrl !== serverUrl || x.FileSystem !== fileSystem);
                    data.Destinations = dtos;

                    var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");
                    this.put(url, JSON.stringify(data), this.fs)
                        .done(() => result.resolve(data))
                        .fail((xhr, statusText, error) => {
                            this.reportError("Could not delete destination (server: " + serverUrl + ", filesystem: " + fileSystem + ")", error, statusText);
                        });
                }
            });

        return result;
    }
}

export = deleteDestinationCommand; 
