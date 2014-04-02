import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");

class getFilesystemDestinationsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {

        var command = new getConfigurationByKeyCommand(this.fs, "Raven/Synchronization/Destinations");
        return command.execute();
    }
}

export = getFilesystemDestinationsCommand;