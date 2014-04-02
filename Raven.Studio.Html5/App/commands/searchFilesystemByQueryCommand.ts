import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class searchFilesystemByQueryCommand  extends commandBase {

    constructor(private fs: filesystem, private queryParameter: string, private sortParameter: string) {
        super();
    }

    execute(): JQueryPromise<filesystemSearchResultsDto> {
        var url = "/search";

        var args = {
            query: this.queryParameter,
            sort: this.sortParameter,
        };

        return this.query<filesystemSearchResultsDto>(url, args, this.fs);
    }
}

export = searchFilesystemByQueryCommand ; 