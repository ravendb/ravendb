import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class downloadFileFromFilesystemCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor() {
        super();

        throw new Error("Not Implemented");
    }

    execute(): JQueryPromise<any> {

        throw new Error("Not Implemented");
    }
}

export = downloadFileFromFilesystemCommand;