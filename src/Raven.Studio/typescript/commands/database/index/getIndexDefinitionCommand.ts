import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexDefinitionCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDefinitionContainerDto> {
        var url = "/indexes";
        var args = {
            name: this.indexName
        }
        return this.query(url, args, this.db, (results: indexDefinitionContainerDto[]) => {
            if (results && results.length) {
                return results[0];
            }
            return null;
        });
    }
}

export = getIndexDefinitionCommand;
