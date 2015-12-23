import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class importFilesystemCommand extends commandBase {

    constructor(private fileData: FormData, private batchSize: number, private stripReplicationInformation: boolean, private shouldDisableVersioiningBundle: boolean, private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Importing data...");

        var args = {
            batchSize: this.batchSize,
            stripReplicationInformation: this.stripReplicationInformation,
            shouldDisableVersioningBundle: this.shouldDisableVersioiningBundle
        }

        var url = "/studio-tasks/import" + this.urlEncodeArgs(args);
        var ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false
        }
        var importTask = this.post(url, this.fileData, this.fs, ajaxOptions); 
        importTask.done(() => this.reportInfo("Data was uploaded successfully, processing..."));
        importTask.fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
        return importTask;
    }
}

export = importFilesystemCommand; 
