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
    private location: databaseLocationSpecifier;

    constructor(private start: boolean, private db: database, options: optsNames | optsType = null, location: databaseLocationSpecifier = undefined) {
        super();
        
        this.location = location;
        
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

        let args: any = {
            ...this.location
        };
        if (this.names) {
            args.name = this.names;
        } else if (this.type) {
            args.type = this.type;
        }

        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        //TODO: report messages!
        return this.post(url, null, this.db, { dataType: undefined });
    }

}

export = togglePauseIndexingCommand;
