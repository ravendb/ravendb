import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class clearIndexErrorsCommand extends commandBase {
    constructor(private indexesNames: string[], private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args =  this.indexesNames ? { name: this.indexesNames } : "";
        const url = endpoints.databases.index.indexesErrors + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully cleared errors from ${this.indexesNames ? 'selected' : 'all'} indexes`));
    }
}

export = clearIndexErrorsCommand;
