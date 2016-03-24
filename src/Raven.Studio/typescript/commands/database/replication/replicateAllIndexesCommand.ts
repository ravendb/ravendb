import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class replicateAllIndexesCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var indexesUrl = '/databases/' + this.db.name + '/replication/replicate-indexes?op=replicate-all';

        this.post(indexesUrl, null, appUrl.getSystemDatabase(), { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate all indexes command!", response.responseText, response.statusText);
                promise.reject();
            }).done(() => {
                this.reportSuccess("Sent replicate all indexes command.");
                promise.resolve();
        });
        
        return promise;
    }
}

export = replicateAllIndexesCommand;
