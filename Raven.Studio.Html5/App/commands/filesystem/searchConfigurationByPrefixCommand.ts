import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class searchFilesystemConfigurationByPrefixCommand extends commandBase {

    constructor(private fs: filesystem, private prefix: string) {
        super();
    }

    execute(): JQueryPromise<filesystemConfigSearchResultsDto> {

        var url = "/config";
        var args = {
            prefix: this.prefix
        }

        return this.query<filesystemConfigSearchResultsDto>(url, args, this.fs);
    }
}

export = searchFilesystemConfigurationByPrefixCommand; 