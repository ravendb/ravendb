import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexPriorityCommand extends commandBase {

    private indexName: string;

    private priority: Raven.Client.Documents.Indexes.IndexPriority;

    private db: database;

    constructor(indexName: string, priority: Raven.Client.Documents.Indexes.IndexPriority, db: database) {
        super();
        this.db = db;
        this.priority = priority;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<void> {
        const payload: Raven.Client.Documents.Operations.Indexes.SetIndexesPriorityOperation.Parameters = {
            Priority: this.priority,
            IndexNames: [this.indexName]
        };
        
        const url = endpoints.databases.index.indexesSetPriority;
        
        return this.post<void>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess(`${this.indexName} Priority was set to ${this.priority}`);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to set index priority", response.responseText));
    }
}

export = saveIndexPriorityCommand; 
