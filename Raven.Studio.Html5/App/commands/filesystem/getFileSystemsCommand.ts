import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getFileSystemsCommand extends commandBase {

    execute(): JQueryPromise<filesystem[]> {
        var args = {
            pageSize: 1024,
            getAdditionalData: true
        };

        var url = "/fs";

        var resultsSelector = (fileSystems: fileSystemDto[]) => fileSystems.map(fs => new filesystem(fs.Name, fs.Disabled));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getFileSystemsCommand; 