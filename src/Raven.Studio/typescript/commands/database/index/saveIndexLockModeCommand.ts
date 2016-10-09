import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import index = require("models/database/index/index");
import endpoints = require("endpoints");

class saveIndexLockModeCommand extends commandBase {

    constructor(private index: index, private lockMode: Raven.Abstractions.Indexing.IndexLockMode, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            mode: this.lockMode,
            name: this.index.name
        };

        const url = endpoints.databases.index.indexesSetLock + this.urlEncodeArgs(args);

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to set index lock mode", response.responseText));
    }
} 

export = saveIndexLockModeCommand;
