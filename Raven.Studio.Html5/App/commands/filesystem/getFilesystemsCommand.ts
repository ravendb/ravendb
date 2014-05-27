import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFilesystemCommand extends commandBase {

    execute(): JQueryPromise<filesystem[]> {
        var resultsSelector = (filesystemNames: any) => filesystemNames.map(n => new filesystem(n));
        return this.query("/fs/names", { pageSize: 1024 }, null, resultsSelector);
    }
}

export = getFilesystemCommand;