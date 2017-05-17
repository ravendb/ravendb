import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: Raven.Client.Documents.Indexes.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.PutIndexResult> {
        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`${this.index.Name} was Saved`);
            });

    }

    private saveDefinition(): JQueryPromise<Raven.Client.Documents.Indexes.PutIndexResult> {
        const url = endpoints.databases.index.indexes;
        const saveTask = $.Deferred<Raven.Client.Documents.Indexes.PutIndexResult>();

        const payload = {
            Indexes: [this.index]
        };

        this.put(url, JSON.stringify(payload), this.db)
            .done((results: Array<Raven.Client.Documents.Indexes.PutIndexResult>) => {
                saveTask.resolve(results[0]);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;

    }
}

export = saveIndexDefinitionCommand; 
