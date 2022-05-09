import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexMapReduceTreeCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier, private indexName: string, private documentIds: Array<string>) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.Debugging.ReduceTree[]> {
        const url = endpoints.databases.index.indexesDebug;
        const args =
        {
            docId: this.documentIds,
            name: this.indexName,
            op: "map-reduce-tree",
            ...this.location
        };
        return this.query(url + this.urlEncodeArgs(args), null, this.db, x => x.Results)
            .fail((response: JQueryXHR) => this.reportError("Failed to load map reduce tree", response.responseText, response.statusText))
            .done((trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) => {
                if (!trees.length) {
                    const documents = this.documentIds.map(x => "'" + x + "'").join(",");
                    this.reportWarning("No results found for " + documents);
                }
            })
    }
} 

export = getIndexMapReduceTreeCommand;
