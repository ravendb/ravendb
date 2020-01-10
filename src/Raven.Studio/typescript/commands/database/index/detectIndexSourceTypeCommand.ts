import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class detectIndexSourceTypeCommand extends commandBase {

    constructor(private index: Raven.Client.Documents.Indexes.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexSourceType> {
        const url = endpoints.databases.studioIndex.studioIndexSourceType;
        
        const task = $.Deferred<Raven.Client.Documents.Indexes.IndexSourceType>();
        
        this.post(url, JSON.stringify(this.index), this.db)
            .done((result: { IndexSourceType: Raven.Client.Documents.Indexes.IndexSourceType}) => {
                task.resolve(result.IndexSourceType);
            }).fail((response: JQueryXHR) => {
                this.reportError("Failed to detect index source type", response.responseText, response.statusText);
                task.reject(response);
            });
        
        return task;
    }
}

export = detectIndexSourceTypeCommand; 
