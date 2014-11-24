import commandBase = require("commands/commandBase");

class createFilesystemCommand extends commandBase {

    /**
    * @param filesystemName The file system name we are creating.
    */
    constructor(private fsSettings: fileSystemSettingsDto) {
        super();

        if (!fsSettings.name) {
            this.reportError("File System must have a name!");
            throw new Error("File System must have a name!");
        }

        if (!$.trim(this.fsSettings.path)) {
            this.fsSettings.path = "~\\Filesystems\\" + this.fsSettings.name;
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating File System '" + this.fsSettings.name + "'");

        var filesystemDoc = {
            "Settings": {
                "Raven/FileSystem/DataDir": this.fsSettings.path
            },
            "Disabled": false
        };
        if (this.fsSettings.storageEngine) {
            filesystemDoc.Settings["Raven/FileSystem/Storage"] = this.fsSettings.storageEngine;
        }

        if ($.trim(this.fsSettings.logsPath)) {
            filesystemDoc.Settings["Raven/TransactionJournalsPath"] = this.fsSettings.logsPath;
        }

        var url = "/admin/fs/" + this.fsSettings.name;
        var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.fsSettings.name + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create file system", response.responseText, response.statusText));

        return createTask;
    }
}

export = createFilesystemCommand;