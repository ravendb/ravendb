import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class synchronizeWithDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private destination: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        throw new Error("Not Implemented");
    }
}

export = synchronizeWithDestinationCommand;  