import commandBase = require("commands/commandBase");
import database = require("models/resources/database");


class resolveAllConflictsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var url = '/replication/forceConflictResolution';

        this.query(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to start conflict resolution task", response.responseText, response.statusText);
                promise.reject();
            }).done(() => {
                this.reportSuccess("Started conflict resolution task (Status->Running Tasks)");
                promise.resolve();
        });
        
        return promise;
    }
}

export = resolveAllConflictsCommand;
