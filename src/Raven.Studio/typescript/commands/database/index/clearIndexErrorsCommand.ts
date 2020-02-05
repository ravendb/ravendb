import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class clearIndexErrorsCommand extends commandBase {    
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = `${endpoints.databases.index.indexesErrors}${this.indexName ? this.urlEncodeArgs({ name: this.indexName }) : ""}`;

        return this.del(url, null, this.db);
    }
}

export = clearIndexErrorsCommand;
