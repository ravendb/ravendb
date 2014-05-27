import commandBase = require("commands/commandBase");

class createCounterStorageCommand extends commandBase {

    /**
    * @param filesystemName The file system name we are creating.
    */
    constructor(private counterStorageName: string, private counterStoragePath: string) {
        super();

        if (!counterStorageName) {
            this.reportError("Counter Storage must have a name!");
            throw new Error("Counter Storage must have a name!");
        }

        if (this.counterStoragePath == null) {
            this.counterStoragePath = "~\\Counters\\" + this.counterStorageName;
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating Counter Storage '" + this.counterStorageName + "'");

        var filesystemDoc = {
            "Settings": { "Raven/Counters/DataDir": this.counterStoragePath },
            "Disabled": false
        };

        var url = "/counterstorage/admin/" + this.counterStorageName;
        
        var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.counterStorageName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create counter storage", response.responseText, response.statusText));

        return createTask;
    }
}

export = createCounterStorageCommand;
