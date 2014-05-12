import appUrl = require("common/appUrl");
import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");

class saveDestinationCommand extends commandBase {

    constructor(private dto: synchronizationReplicationsDto, private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Replication destinations.");
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved Replication destinations."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Replication destinations.", response.responseText, response.statusText));

        //var result = $.Deferred();

        //var dtos = [].concat(this.destination);

        //this.query<synchronizationDestinationDto[]>("/config", { name: "Raven/Synchronization/Destinations" }, this.fs)
        //    .done(data => {
                
        //        if (data && data.hasOwnProperty('destination')) {      

        //            var value = data['destination'];
        //            if (!(value instanceof Array))
        //                value = [value];
                                                                                         
        //            dtos = dtos.concat(value.map(x => <synchronizationDestinationDto> x));                    
        //        }
        //    })
        //    .always(x => {
        //        var url = "/config?name=" + encodeURIComponent("Raven/Synchronization/Destinations");       

        //        var doc = {
        //            destination: dtos
        //        };

        //        var docAsString = JSON.stringify(doc);

        //        this.put(url, docAsString, this.fs)
        //            .done(y => {
        //                result.resolve(dtos);
        //            });
        //    });

        //return result;
    }

    private saveSetup(): JQueryPromise<any> {
        var name = "Raven/Synchronization/Destinations";
        var url = "/config?name=" + encodeURIComponent(name);
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, this.fs);
    }
}

export = saveDestinationCommand;