import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCSharpIndexDefinitionCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        const args =
        {
            name: this.indexName
        };
        const url = endpoints.databases.index.indexesCSharpIndexDefinition + this.urlEncodeArgs(args);
        return this.query(url, null, this.db, null, { dataType: "text" });
    }
}

export = getCSharpIndexDefinitionCommand;
