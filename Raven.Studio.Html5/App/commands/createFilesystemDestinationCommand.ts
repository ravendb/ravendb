import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");
import getFilesystemConfigurationByKeyCommand = require("commands/getFilesystemConfigurationByKeyCommand");
import saveFilesystemConfigurationCommand = require("commands/saveFilesystemConfigurationCommand");

class createFilesystemDestinationCommand extends commandBase {

    constructor(private fs: filesystem, private url: string) {
        super();
    }

    execute(): JQueryPromise<any> {

        //var result = $.Deferred;

        //var getCommand = new getFilesystemConfigurationByKeyCommand(filesystem, "Raven/Synchronization/Destinations");
        //getCommand.execute()
        //    .done(x => {
        //        x.addKey("url", this.url);

        //        result = new saveFilesystemConfigurationCommand(filesystem, "Raven/Synchronization/Destinations", x)
        //            .execute();
        //    });    

        //return result;

        throw new Error("Not Implemented");
    }
}

export = createFilesystemDestinationCommand;