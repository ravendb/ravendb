import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

type killByIndexAndQueryId = {
    indexName: string,
    id: number
}

type killByClientQueryId = {
    clientQueryId: string
}

type killArgs = killByIndexAndQueryId | killByClientQueryId;

class killQueryCommand extends commandBase {

    private constructor(private db: database, private args: killArgs) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.queriesDebug.debugQueriesKill;

        const task = $.Deferred<void>();

        this.post(url + this.urlEncodeArgs(this.args), null, this.db, { dataType: undefined })
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
    
    static byClientQueryId(db: database, clientQueryId: string) {
        return new killQueryCommand(db, {
            clientQueryId
        });
    }
    
    static byIndexAndQueryId(db: database, indexName: string, queryId: number) {
        return new killQueryCommand(db, {
            indexName,
            id: queryId
        });
    }
}

export = killQueryCommand;
