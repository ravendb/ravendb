import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class replicateAllTransformersCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var transformersUrl = '/databases/' + this.db.name + '/replication/replicate-transformers?op=replicate-all';

        this.post(transformersUrl, null, appUrl.getSystemDatabase(), { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate all transformers command!", response.responseText, response.statusText);
                promise.reject();
            }).done(() => {
                this.reportSuccess("Sent replicate all transformers command");
                promise.resolve();
            });

        return promise;
    }
}

export = replicateAllTransformersCommand; 
