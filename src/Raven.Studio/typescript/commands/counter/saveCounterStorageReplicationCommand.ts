import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class saveCounterStorageReplicationCommand extends commandBase {

    constructor(private dto: counterStorageReplicationDto, private counterStorage: counterStorage) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving counters replication");
        
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved counters replication"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save counters replication", response.responseText, response.statusText));
    }

    private saveSetup(): JQueryPromise<any> {
        var url = "/replications/save";
        var putArgs = JSON.stringify(this.dto);
        return this.post(url, putArgs, this.counterStorage, { dataType: undefined });
    }
}

export = saveCounterStorageReplicationCommand;
