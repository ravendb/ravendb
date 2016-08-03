import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import replicationDestination = require("models/database/replication/replicationDestination");

class replicateIndexesCommand extends commandBase {

    constructor(private db: database, private destination: replicationDestination) {
        super();
    }

    execute(): JQueryPromise<void> {
        var indexesUrl = '/databases/' + this.db.name + '/replication/replicate-indexes?op=replicate-all-to-destination';
        var destinationJson = JSON.stringify(this.destination.toDto());
        return this.post(indexesUrl, destinationJson, appUrl.getSystemDatabase(), { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate indexes command!", response.responseText, response.statusText);
            }).done(() => {
                this.reportSuccess("Sent replicate indexes command.");
            });
    }
}

export = replicateIndexesCommand; 
