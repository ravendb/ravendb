import commandBase = require("commands/commandBase");
import file = require("models/file");
import fileMetadata = require("models/fileMetadata");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class getFileCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<file> {
        var url = "/files/" + this.name;
        var resultsSelector = metadata => {
            var fileHeaders = new file();
            fileHeaders.id = this.name;
            fileHeaders.__metadata = new fileMetadata(metadata);
            return fileHeaders;
        }
        return this.head(url, null, this.fs, resultsSelector);
    }

}

export = getFileCommand; 