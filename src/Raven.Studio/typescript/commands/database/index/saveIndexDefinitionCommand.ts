import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: Raven.Client.Indexing.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Data.Indexes.PutIndexResult> {
        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`${this.index.Name} was Saved`);
            });

    }

    private saveDefinition(): JQueryPromise<Raven.Client.Data.Indexes.PutIndexResult> {
        const payload = JSON.stringify([this.index]);
        const url = endpoints.databases.index.indexes;
        const saveTask = $.Deferred<Raven.Client.Data.Indexes.PutIndexResult>();
        this.put(url, payload, this.db)
            .done((results: Array<Raven.Client.Data.Indexes.PutIndexResult>) => {
                saveTask.resolve(results[0]);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;

    }
}

export = saveIndexDefinitionCommand; 
