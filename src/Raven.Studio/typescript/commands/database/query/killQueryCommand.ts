import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class killQueryCommand extends commandBase {

    constructor(private db: database, private indexName: string, private queryId: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.queryId,
            indexName: this.indexName
        };
        const url = endpoints.databases.queriesDebug.debugQueriesKill;

        const task = $.Deferred<void>();

        this.post(url + this.urlEncodeArgs(args), null, this.db, { dataType: undefined })
            .done(result => task.resolve())
            .fail((response: JQueryXHR) => {
                if (response.status === 404) {
                    task.resolve(null);
                } else {
                    this.reportError("Failed to kill the query", response.responseText, response.statusText);
                    task.reject();
                }
            });

        return task;
    }
}

export = killQueryCommand;
