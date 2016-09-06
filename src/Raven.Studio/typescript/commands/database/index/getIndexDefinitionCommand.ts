import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexDefinitionCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDefinitionContainerDto> {
        var url = endpoints.databases.index.indexes;
        var args = {
            name: this.indexName
        }
        return this.query(url, args, this.db, (results: any[]) => {
            if (results && results.length) {
                return {
                    Index: results[0]
                };
            }
            return null;
        });
    }
}

export = getIndexDefinitionCommand;
