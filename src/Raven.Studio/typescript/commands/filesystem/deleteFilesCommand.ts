import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class deleteFilesCommand extends commandBase {

    constructor(private fileIds: string[], private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deletionTasks = new Array<JQueryPromise<any>>();
        for (var i = 0; i < this.fileIds.length; i++) {
            var deleteCommand = this.deleteFile(this.fileIds[i]);
            deletionTasks.push(deleteCommand);
        }

        var successMessage = this.fileIds.length > 1 ? "Deleted " + this.fileIds.length + " files" : "Deleted " + this.fileIds[0];

        var combinedTask = $.when.apply($, deletionTasks)
            .done(() => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete files", response.responseText, response.statusText));

        return combinedTask;
    }

    deleteFile(fileId : string): JQueryPromise<any> {
        var url = "/files/" + encodeURIComponent(fileId);
        return this.del(url, null, this.fs, null, 9000 * this.fileIds.length);
    }

}

export = deleteFilesCommand;  
