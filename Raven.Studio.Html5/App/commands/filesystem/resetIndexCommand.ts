import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class resetIndexCommand extends commandBase {

    constructor(private fs:filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/admin/reset-index";
        return this.post(url, null, this.fs, { dataType: 'text' })
            .done(() => this.reportSuccess("File system index successfully reset"))
            .fail((response: JQueryXHR) => this.reportError("Failed to reset file system index.", response.responseText, response.statusText));
    }

}


export = resetIndexCommand;
