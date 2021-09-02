import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

type optsNames = {
    name: Array<string>;
}

type toggleType = "map" | "map-reduce";

type optsType = {
    type: toggleType;
}

class togglePauseIndexingCommand extends commandBase {

    private toggleAll = false;
    private names: Array<string>;
    private type: toggleType;

    constructor(private start: boolean, private db: database, options: optsNames | optsType = null) {
        super();
        if (options && "name" in options) {
            this.names = options.name;
        } else if (options && "type" in options) {
            this.type = options.type;
        } else {
            this.toggleAll = true;
        }
    }

    execute(): JQueryPromise<void> {
        const basicUrl = this.start ? endpoints.databases.adminIndex.adminIndexesStart : endpoints.databases.adminIndex.adminIndexesStop;

        let args: optsNames | optsType = null;
        if (this.names) {
            args = { name: this.names };
        } else if (this.type) {
            args = { type: this.type };
        }

        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                const state = this.start ? "Resumed" : "Paused until restart";
                const indexNameMessage = this.names ? this.names[0] : "Indexing";
                this.reportSuccess(`${indexNameMessage} was ${state}`);
             })
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle indexing status", response.responseText));
    }

}

export = togglePauseIndexingCommand;
