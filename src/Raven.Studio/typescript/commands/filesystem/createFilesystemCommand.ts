import commandBase = require("commands/commandBase");

class createFilesystemCommand extends commandBase {

    /**
    * @param filesystemName The file system name we are creating.
    */
    constructor(private filesystemName: string, private settings: {}, private securedSettings: {}) {
        super();

        if (!filesystemName) {
            this.reportError("File System must have a name!");
            throw new Error("File System must have a name!");
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating File System '" + this.filesystemName + "'");


        var filesystemDoc = {
            "Settings": this.settings,
            "SecuredSettings": this.securedSettings, // TODO: based on the selected bundles, we may need to include additional settings here
            "Disabled": false
        };

        var url = "/admin/fs/" + this.filesystemName;
        var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.filesystemName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create file system", response.responseText, response.statusText));

        return createTask;
    }
}

export = createFilesystemCommand;
