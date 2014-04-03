import commandBase = require("commands/commandBase");
import file = require("models/file");
import fileMetadata = require("models/fileMetadata");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class deleteFileCommand extends commandBase {

    constructor(private fs: filesystem, private name: string) {
        super();
    }

    execute(): JQueryPromise<file> {
        var url = "/files/" + this.name;
        return this.del(url, null, this.fs)
            .done(x => this.reportSuccess("File " + this.name + " was successfully deleted."))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete " + this.name, response.responseText, response.statusText));
    }

}

export = deleteFileCommand;  