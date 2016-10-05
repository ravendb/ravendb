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

class toggleIndexingCommand extends commandBase {

    private toggleAll = false;
    private names: Array<string>;
    private type: toggleType;

    constructor(private start: boolean, private db: database, options: optsNames | optsType = null) {
        super();
        if (options && options.hasOwnProperty("name")) {
            this.names = (options as optsNames).name;
        } else if (options && options.hasOwnProperty("type")) {
            this.type = (options as optsType).type;
        } else {
            this.toggleAll = true;
        }
    }

    execute(): JQueryPromise<void> {
        const basicUrl = this.start ? endpoints.databases.index.indexesStart : endpoints.databases.index.indexesStop;

        let args: optsNames | optsType = null;
        if (this.names) {
            args = { name: this.names } as optsNames;
        } else if (this.type) {
            args = { type: this.type } as optsType;
        }

        this.reportInfo(this.getOperationStartMessage());
        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess(this.getSuccessMessage()))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle indexing status", response.responseText));
    }

    private getOperationStartMessage() {
        if (this.toggleAll || this.type || (this.names && this.names.length !== 1)) {
            return this.start ? "Resuming indexing": "Pausing indexing";
        }

        return this.start ? "Resuming indexing of " + this.names[0] : "Pausing indexing of " + this.names[0];
    }

    private getSuccessMessage() {
        if (this.toggleAll || this.type || (this.names && this.names.length !== 1)) {
            return this.start ? "Resumed indexing" : "Paused indexing";
        }

        return this.start ? "Resumed indexing of " + this.names[0] : "Paused indexing of " + this.names[0];
    }
}

export = toggleIndexingCommand;