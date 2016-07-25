import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class updateFileMetadataCommand extends commandBase {

    constructor(private fileName: string, private metadata: any, private fs: filesystem, private reportSaveProgress = true) {
        super();
    }

    execute(): JQueryPromise<any> {
        if (this.reportSaveProgress) {
            this.reportInfo("Saving " + this.fileName + "...");
        }

        var customHeaders: any = {};

        //We only want to stringify when the header's value is a json doc.
        for (var key in this.metadata) {
            var value = this.metadata[key];
            if (typeof (value) != "string" && typeof (value) != "number")
                customHeaders[key] = JSON.stringify(value);
            else {
                customHeaders[key] = this.metadata[key];
            }
        }

        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders
        };

        var url = "/files/" + encodeURIComponent(this.fileName);
        var updateTask = this.post(url, null, this.fs, jQueryOptions);

        if (this.reportSaveProgress) {
            updateTask.done(() => this.reportSuccess("Saved " + this.fileName));
            updateTask.fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.fileName, response.responseText, response.statusText);
            });
        }

        return updateTask;
    }
}

export = updateFileMetadataCommand;
