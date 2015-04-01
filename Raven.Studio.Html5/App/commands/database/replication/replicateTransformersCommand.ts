import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import replicationDestination = require("models/database/replication/replicationDestination");

class replicateTransformersCommand extends commandBase {
    private destination: replicationDestination;
    constructor(private db: database, destination: replicationDestination) {
        super();
        this.destination = destination;
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        
        var transformersUrl = '/databases/' + this.db.name + '/replication/replicate-transformers?op=replicate-all-to-destination';
        var destinationJson = JSON.stringify(this.destination.toDto());
        this.post(transformersUrl, destinationJson, appUrl.getSystemDatabase())
            .fail((response: JQueryXHR) => {
            this.reportError("Failed to send replicate transformers command!", response.responseText, response.statusText);
            promise.reject();
        }).done(() => {
                this.reportSuccess("Sent replicate transformers command");
                promise.resolve();
        });
        
        return promise;
    }
}

export = replicateTransformersCommand;  