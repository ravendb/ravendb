import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class synchronizeWithDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var synchronizeUrl = "/synchronization/ToDestination?destination=" + this.destination + "&forceSyncingAll=true";

        return this.post(synchronizeUrl, null, this.fs)
            .fail(x => {
                    this.reportError("Synchronization endpoint returned with an error.")
                });
    }
}

export = synchronizeWithDestinationCommand;  