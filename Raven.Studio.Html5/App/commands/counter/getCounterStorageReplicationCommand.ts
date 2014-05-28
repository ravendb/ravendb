import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class getCounterStorageReplicationCommand extends commandBase {

    constructor(private counterStorage: counterStorage, private reportRefreshProgress = false) {
        super();
        if (!counterStorage) {
            throw new Error("Must specify counter storage");
        }
    }

    execute(): JQueryPromise<counterStorageReplicationDestinatinosDto> {
        var url = "/replications/get";
        var getTask = this.query(url, null, this.counterStorage);

        if (this.reportRefreshProgress) {
            getTask.done(() => this.reportSuccess("Replication Destionations of '" + this.counterStorage.name + "' were successfully refreshed!"));
            getTask.fail((response: JQueryXHR) => this.reportWarning("There are no saved replication destionations on the server!", response.responseText, response.statusText));
        }
        return getTask;
    }
}

export = getCounterStorageReplicationCommand;