import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveQueueSinkCommand extends commandBase {
    
    private readonly db: database;
    private readonly payload: Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration;

    constructor(db: database, payload: Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration) {
        super();
        this.payload = payload;
        this.db = db;
    }  

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.updateEtl()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to save Queue Sink task`, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved Queue Sink task`); 
            });
    }

    private updateEtl(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        
        const args = {
            id : this.payload.TaskId || undefined,
        };
        
        const url = endpoints.databases.ongoingTasks.adminQueueSink + this.urlEncodeArgs(args);
        
        return this.put(url, JSON.stringify(this.payload), this.db);
    }
}

export = saveQueueSinkCommand;
