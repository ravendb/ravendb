import commandBase = require("commands/commandBase");
import database = require("models/database");
import index = require("models/index");

class saveIndexAsPersistentCommand extends commandBase {

    constructor(private indexToPersist: index, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Persisting " + this.indexToPersist.name + "...");

        // Deliberately not URL-encoding index name, as it causes the operation to fail on the server.
        var url = "/indexes/" + this.indexToPersist.name + "?op=forceWriteToDisk";
        var saveTask = this.post(url, null, this.db);
        saveTask.done(() => {
            this.reportSuccess("Persisted " + this.indexToPersist.name);
            this.indexToPersist.isOnRam("false");
        });
        saveTask.fail(details => this.reportError("Failed to persist " + this.indexToPersist.name, JSON.stringify(details)));

        return saveTask;
    }
}

export = saveIndexAsPersistentCommand;