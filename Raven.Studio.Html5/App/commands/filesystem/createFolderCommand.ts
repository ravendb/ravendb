import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import collection = require("models/collection");

class createFilesystemFolderCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();

        throw new Error("Not Implemented");
    }

    execute(): JQueryPromise<collection[]> {

        throw new Error("Not Implemented");
    }
}

export = createFilesystemFolderCommand;