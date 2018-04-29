import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class resetSynchronizationCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var synchronizeUrl = "/synchronization/resetDestinations";

        return this.post(synchronizeUrl, null, this.fs)
                .fail(x => {
                    })
    }
}

export = resetSynchronizationCommand;   
