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
            name: "Raven/Synchronization/Destinations"
        };

        return this.query<synchronizationReplicationsDto>(url, args, this.fs);
    }
}

export = getDestinationsCommand;