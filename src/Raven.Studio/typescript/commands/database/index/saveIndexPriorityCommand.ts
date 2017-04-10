import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexPriorityCommand extends commandBase {

    constructor(private indexName: string, private priority: Raven.Client.Documents.Indexes.IndexPriority, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName, 
            priority: this.priority
        }
        const url = endpoints.databases.index.indexesSetPriority + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess(`${this.indexName} Priority was set to ${args.priority}`);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to set index priority", response.responseText));
    }
}

export = saveIndexPriorityCommand; 
