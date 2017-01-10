import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: Raven.Client.Indexing.IndexDefinition, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`${this.index.Name} was Saved`);
            });

    }

    private saveDefinition(): JQueryPromise<saveIndexResult> {
        const args = {
            name: this.index.Name
        };
        const payload = JSON.stringify(this.index);
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);
        return this.put(url, payload, this.db);
    }
}

export = saveIndexDefinitionCommand; 
