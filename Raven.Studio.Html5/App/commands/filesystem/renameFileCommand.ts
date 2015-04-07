import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class renameFileCommand extends commandBase {

    constructor(private fs: filesystem, private oldName: string, private newName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/files/" + encodeURIComponent(this.oldName) + "?rename=" + encodeURIComponent(this.newName);

        return this.patch(url, null, this.fs)
            .done(() => this.reportSuccess("Successfully renamed file"))
            .fail((response: JQueryXHR) => this.reportError("Unable to rename file name", response.responseText, response.statusText));
    }
}

export = renameFileCommand;