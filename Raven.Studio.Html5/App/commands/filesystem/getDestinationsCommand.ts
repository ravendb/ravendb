import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");

class getFilesystemDestinationsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<synchronizationDestinationDto[]> {

        var url = "/config";
        var args = {
            name: "Raven/Synchronization/Destinations",
        };

        var task = $.Deferred();
        this.query<any>(url, args, this.fs)
            .done(data => {
                if (data.hasOwnProperty('destination')) {

                    var value = data['destination'];
                    if (!(value instanceof Array))
                        value = [value];

                    var result = value.map(x => <synchronizationDestinationDto> x);
                    task.resolve(result);
                }
                else {
                    task.resolve([]);
                }
            })
            .fail((qXHR, textStatus, errorThrown) => {
                if (qXHR.status != "404") {
                    this.reportError("Could not get synchronization destinations.", errorThrown, textStatus);
                }
                task.resolve([]);
            });

        return task;
    }
}

export = getFilesystemDestinationsCommand;