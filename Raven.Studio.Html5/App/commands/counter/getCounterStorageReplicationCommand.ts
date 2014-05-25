import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class getCounterStorageReplicationCommand extends commandBase {

    constructor(private counterStorage: counterStorage) {
        super();
        if (!counterStorage) {
            throw new Error("Must specify counter storage");
        }
    }

    execute(): JQueryPromise<counterStorageReplicationDestinatinosDto> {
        var url = "/replications-get";
        return this.query(url, null, this.counterStorage);
    }
}

export = getCounterStorageReplicationCommand;