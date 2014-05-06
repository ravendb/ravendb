import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class synchronizeNowCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var synchronizeUrl = "/synchronization/ToDestinations?forceSyncingAll=true";

        return this.post(synchronizeUrl, null, this.fs)
                .fail(x => {
                    this.reportError("Synchronization endpoint returned with an error.")
                    })
    }
}

export = synchronizeNowCommand;   