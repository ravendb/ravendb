import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

//TODO: do we need it?
class tryRecoverCorruptedIndexes extends commandBase {

    constructor(private db:database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/indexes/try-recover-corrupted";//TODO: use endpoints
        return this.patch(url, null, this.db)
            .done(() => this.reportSuccess("The recovery operation started in the background"))
            .fail((response: JQueryXHR) => this.reportError("Failed to start recovery operation", response.responseText, response.statusText));
    }
}

export = tryRecoverCorruptedIndexes;
