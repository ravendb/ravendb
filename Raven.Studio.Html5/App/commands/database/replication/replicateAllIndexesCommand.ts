import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class replicateAllIndexesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        var indexesUrl = '/databases/' + this.db.name + '/replication/replicate-indexes?op=replicate-all';

        return this.post(indexesUrl, null, appUrl.getSystemDatabase(), { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate all indexes command!", response.responseText, response.statusText);
            }).done(() => {
                this.reportSuccess("Sent replicate all indexes command.");
            });
    }
}

export = replicateAllIndexesCommand;
