import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import appUrl = require("common/appUrl");

class uploadFileToFilesystemCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private source: File, private uploadId: string, private fs: filesystem, progressHandlingFunction: (evt: any) => void, private reportUploadProgress = true) {
        super();
    }

    execute(): JQueryPromise<any> {
        if (this.reportUploadProgress) {
            this.reportInfo("File " + this.source.name + "queued for upload...");
        }

        var url = '/files?name=' + this.source.name + '&uploadId=' + this.uploadId;

        var customHeaders = {
            'RavenFS-Size': this.source.size
        };

        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders,
            processData: false,
            xhr: function () {  // Custom XMLHttpRequest
                var myXhr = $.ajaxSettings.xhr();
                if (myXhr.upload) { // Check if upload property exists
                    myXhr.upload.addEventListener('progress', this.progressHandlingFunction, false); // For handling the progress of the upload
                }
                if (myXhr.upload) {
                    myXhr.upload.onprogress = this.progressHandlingFunction;
                }
                return myXhr;
            },
            cache: false,
            contentType: false,
            dataType: ''
        };


        var uploadTask = this.put(url, this.source, this.fs, jQueryOptions);

        if (this.reportUploadProgress) {
            uploadTask.done(() => this.reportSuccess("Uploaded " + this.source.name));
            uploadTask.fail((response: JQueryXHR) => this.reportError("Failed to upload " + this.source.name, response.responseText, response.statusText));
        }

        return uploadTask;
    }
}

export = uploadFileToFilesystemCommand;