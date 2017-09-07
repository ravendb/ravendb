import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import index = require("models/database/index/index");
import endpoints = require("endpoints");

class saveIndexLockModeCommand extends commandBase {

    constructor(private indexes: Array<index>, private lockMode: Raven.Client.Documents.Indexes.IndexLockMode, private db: database, private lockTitle: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const payload = {
            Mode: this.lockMode,
            IndexNames: this.indexes.map(x => x.name)
        } as Raven.Client.Documents.Operations.Indexes.SetIndexesLockOperation.Parameters;

        const url = endpoints.databases.index.indexesSetLock;

        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .done(() => {
                const indexesNameStr = this.indexes.length === 1 ? this.indexes[0].name : "Indexes";
                this.reportSuccess(`${indexesNameStr} mode was set to: ${this.lockTitle}`);
             })
            .fail((response: JQueryXHR) => this.reportError("Failed to set index lock mode", response.responseText));
    }
} 

export = saveIndexLockModeCommand;
