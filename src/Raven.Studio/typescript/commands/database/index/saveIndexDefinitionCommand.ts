import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: Raven.Client.Documents.Indexes.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`${this.index.Name} was Saved`);
            });
    }

    private saveDefinition(): JQueryPromise<string> {
        const url = endpoints.databases.adminIndex.adminIndexes;
        const saveTask = $.Deferred<string>();

        const payload = {
            Indexes: [this.index]
        };

        this.put(url, JSON.stringify(payload), this.db)
            .done((results: any) => {
                saveTask.resolve(results["Results"][0].Index);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveIndexDefinitionCommand; 
