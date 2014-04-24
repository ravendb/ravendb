import commandBase = require("commands/commandBase");

class createFilesystemCommand extends commandBase {

    /**
    * @param filesystemName The file system name we are creating.
    */
    constructor(private filesystemName: string, private filesystemPath: string) {
        super();

        if (!filesystemName) {
            this.reportError("File System must have a name!");
            throw new Error("File System must have a name!");
        }

        if (this.filesystemPath == null) {
            this.filesystemPath = "~\\Filesystems\\" + this.filesystemName;
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating File System '" + this.filesystemName + "'");

        var filesystemDoc = {
            "Settings": { "Raven/FileSystem/DataDir" : this.filesystemPath },
            "Disabled": false
        };

        var url = "/ravenfs/admin/" + this.filesystemName;
        var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.filesystemName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create file system", response.responseText, response.statusText));

        return createTask;
    }
}

export = createFilesystemCommand;