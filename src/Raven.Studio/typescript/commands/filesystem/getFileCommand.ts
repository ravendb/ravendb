import commandBase = require("commands/commandBase");
import file = require("models/filesystem/file");
import fileMetadata = require("models/filesystem/fileMetadata");
import filesystem = require("models/filesystem/filesystem");

class getFileCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<file> {
        var url = "/files/" + encodeURIComponent(this.name);   
        var resultsSelector = (metadata: any) => {
            var fileHeaders = new file();
            fileHeaders.id(this.name);

            for (var property in metadata) {
                var value: string = metadata[property];
                if (value[0] === "{" || value[0] === "[")
                    metadata[property] = JSON.parse(value);
            }

            fileHeaders.__metadata = new fileMetadata(metadata);
            return fileHeaders;
        };
        return this.head(url, null, this.fs, resultsSelector);
    }

}

export = getFileCommand; 
