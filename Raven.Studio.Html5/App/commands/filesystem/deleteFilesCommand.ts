import commandBase = require("commands/commandBase");
import file = require("models/filesystem/file");
import fileMetadata = require("models/filesystem/fileMetadata");
import filesystem = require("models/filesystem/filesystem");
import pagedResultSet = require("common/pagedResultSet");

class deleteFilesCommand extends commandBase {

    constructor(private fileIds: string[], private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deletionTasks = [];
        for (var i = 0; i < this.fileIds.length; i++) {
            var deleteCommand = this.deleteFile(this.fileIds[i]);
            deletionTasks.push(deleteCommand);
        }

        var successMessage = this.fileIds.length > 1 ? "Deleted " + this.fileIds.length + " files" : "Deleted " + this.fileIds[0];

        var combinedTask = $.when(deletionTasks)
            .done(x => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete files", response.responseText, response.statusText));

        return combinedTask;
    }

    deleteFile(fileId : string): JQueryPromise<any> {
        var url = "/files/" + fileId;
        return this.del(url, null, this.fs)

    }

}

export = deleteFilesCommand;  