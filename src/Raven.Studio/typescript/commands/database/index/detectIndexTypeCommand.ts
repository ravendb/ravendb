import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class detectIndexTypeCommand extends commandBase {

    constructor(private index: Raven.Client.Documents.Indexes.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexType> {
        const url = endpoints.databases.studioIndex.studioIndexType;
        
        const task = $.Deferred<Raven.Client.Documents.Indexes.IndexType>();
        
        this.post(url, JSON.stringify(this.index), this.db)
            .done((result: { IndexType: Raven.Client.Documents.Indexes.IndexType}) => {
                task.resolve(result.IndexType);
            }).fail((response: JQueryXHR) => {
                this.reportError("Failed to detect index type", response.responseText, response.statusText);
                task.reject(response);
            });
        
        return task;
    }
}

export = detectIndexTypeCommand; 
