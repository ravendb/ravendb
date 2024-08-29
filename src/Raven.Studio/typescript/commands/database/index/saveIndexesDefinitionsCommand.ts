import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;

class saveIndexesDefinitionsCommand extends commandBase {
    private readonly databaseName: string;
    private readonly indexes: IndexDefinition[];

    constructor(databaseName: string, indexes: IndexDefinition[]) {
        super();
        this.databaseName = databaseName;
        this.indexes = indexes;
    }

    execute(): JQueryPromise<any> {
        const url = endpoints.databases.adminIndex.adminIndexes;

        const payload = {
            Indexes: this.indexes,
        };

        return this.put(url, JSON.stringify(payload), this.databaseName)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save indexes", response.responseText, response.statusText);
            })
            .then(() => {
                this.reportSuccess("Indexes were saved");
            });
    }
}

export = saveIndexesDefinitionsCommand;
