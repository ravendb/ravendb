import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem");

class uploadFileToFilesystemCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private fileName: string, private uploadId: string, private fs : filesystem) {
        super();

        throw new Error("Not Implemented");
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Uploading file " +this.fileName+"...");

        //var url = '/files?name=' + this.fileName + '&uploadId=' + this.uploadId;
        //var createTask = this.put(url, null, this.fs);

        //return createTask;
    }
}

export = uploadFileToFilesystemCommand;