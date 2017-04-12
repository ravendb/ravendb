import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import index = require("models/database/index/index");
import endpoints = require("endpoints");

class saveIndexLockModeCommand extends commandBase {

    constructor(private indexes: Array<index>, private lockMode: Raven.Client.Documents.Indexes.IndexLockMode, private db: database, private lockTitle: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            mode: this.lockMode,
            name: this.indexes.map(x => x.name)
        };

        const url = endpoints.databases.index.indexesSetLock + this.urlEncodeArgs(args);

        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                const indexesNameStr = this.indexes.length === 1 ? this.indexes[0].name : "Indexes";
                this.reportSuccess(`${indexesNameStr} mode was set to: ${this.lockTitle}`);
             })
            .fail((response: JQueryXHR) => this.reportError("Failed to set index lock mode", response.responseText));
    }
} 

export = saveIndexLockModeCommand;
