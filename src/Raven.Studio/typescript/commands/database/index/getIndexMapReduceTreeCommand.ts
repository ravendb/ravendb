import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexMapReduceTreeCommand extends commandBase {

    constructor(private db: database, private indexName: string, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.Debugging.ReduceTree[]> {
        const url = endpoints.databases.index.indexesDebug;
        const args =
        {
            docID: this.documentId,
            name: this.indexName,
            op: "map-reduce-tree"
        };
        return this.query(url, args, this.db);
    }
} 

export = getIndexMapReduceTreeCommand;
