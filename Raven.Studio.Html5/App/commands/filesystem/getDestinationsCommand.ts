import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");
import document = require("models/document");

class getDestinationsCommand extends commandBase {

    shouldResolveNotFoundAsNull: boolean;

    constructor(private fs: filesystem, shouldResolveNotFoundAsNull?: boolean) {
        super();
        if (!fs) {
            throw new Error("Must specify filesystem");
        }
        this.shouldResolveNotFoundAsNull = shouldResolveNotFoundAsNull || false;
    }

    execute(): JQueryPromise<synchronizationReplicationsDto> {

        var url = "/config";
        var args = {
            name: "Raven/Synchronization/Destinations",
        };

        var documentResult = $.Deferred();
        var result = this.query<any>(url, args, this.fs);
        result.fail(xhr => documentResult.fail(xhr));
        result.done((queryResult: queryResultDto) => {
            if (queryResult.Results.length === 0) {
                if (this.shouldResolveNotFoundAsNull) {
                    documentResult.resolve(null);
                } else {
                    documentResult.reject("Unable to find document with ID " + args.name);
                }
            } else {
                documentResult.resolve(new document(queryResult.Results[0]));
            }
        });

        return documentResult;
    }

    //constructor(private fs: filesystem) {
    //    super();
    //}

    //execute(): JQueryPromise<synchronizationDestinationDto[]> {

    //    var url = "/config";
    //    var args = {
    //        name: "Raven/Synchronization/Destinations",
    //    };

    //    var task = $.Deferred();
    //    this.query<any>(url, args, this.fs)
    //        .done(data => {
    //            if (data.hasOwnProperty('destination')) {

    //                var value = data['destination'];
    //                if (!(value instanceof Array))
    //                    value = [value];

    //                var result = value.map(x => <synchronizationDestinationDto> x);
    //                task.resolve(result);
    //            }
    //            else {
    //                task.resolve([]);
    //            }
    //        })
    //        .fail((qXHR, textStatus, errorThrown) => {
    //            if (qXHR.status != "404") {
    //                this.reportError("Could not get synchronization destinations.", errorThrown, textStatus);
    //            }
    //            task.resolve([]);
    //        });

    //    return task;
    //}
}

export = getDestinationsCommand;