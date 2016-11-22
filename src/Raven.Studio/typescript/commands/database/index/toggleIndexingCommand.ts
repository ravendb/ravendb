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
        const basicUrl = this.start ? endpoints.databases.adminIndex.adminIndexesStart : endpoints.databases.adminIndex.adminIndexesStop;

        let args: optsNames | optsType = null;
        if (this.names) {
            args = { name: this.names } as optsNames;
        } else if (this.type) {
            args = { type: this.type } as optsType;
        }

        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle indexing status", response.responseText));
    }

}

export = toggleIndexingCommand;