import commandBase = require("commands/commandBase");

class createFilesystemCommand extends commandBase {

    /**
    * @param filesystemName The file system name we are creating.
    */
    constructor(private fileSystemName: string, private fileSystemPath: string) {
        super();

        if (!fileSystemName) {
            this.reportError("File System must have a name!");
            throw new Error("File System must have a name!");
        }

        if (this.fileSystemPath == null) {
            this.fileSystemPath = "~\\Filesystems\\" + this.fileSystemPath;
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating File System '" + this.fileSystemName + "'");

        var filesystemDoc = {
            "Settings": { "Raven/FileSystem/DataDir": this.fileSystemPath },
            "Disabled": false
        };

        var url = "/fs/admin/" + this.fileSystemName;
        var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.fileSystemName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create file system", response.responseText, response.statusText));

        return createTask;
    }
}

export = createFilesystemCommand;