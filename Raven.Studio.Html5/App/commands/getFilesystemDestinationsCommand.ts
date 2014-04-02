import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");
import getFilesystemConfigurationByKeyCommand = require("commands/getFilesystemConfigurationByKeyCommand");

class getFilesystemDestinationsCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {

        var command = new getFilesystemConfigurationByKeyCommand(this.fs, "Raven/Synchronization/Destinations");
        return command.execute();
    }
}

export = getFilesystemDestinationsCommand;