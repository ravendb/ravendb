import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");
import collection = require("models/collection");

class createFilesystemCommand extends commandBase {

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

export = createFilesystemCommand;