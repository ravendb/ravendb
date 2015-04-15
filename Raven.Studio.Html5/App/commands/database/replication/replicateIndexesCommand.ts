import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import replicationDestination = require("models/database/replication/replicationDestination");

class replicateIndexesCommand extends commandBase {
    private destination: replicationDestination;
    constructor(private db: database, destination: replicationDestination) {
        super();
        this.destination = destination;
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();

        var indexesUrl = '/databases/' + this.db.name + '/replication/replicate-indexes?op=replicate-all-to-destination';
        var destinationJson = JSON.stringify(this.destination.toDto());
        this.post(indexesUrl, destinationJson, appUrl.getSystemDatabase())
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate indexes command!", response.responseText, response.statusText);
                promise.reject();
            }).done(() => {
                this.reportSuccess("Sent replicate indexes command.");
                promise.resolve();
            });

        return promise;
    }
}

export = replicateIndexesCommand; 