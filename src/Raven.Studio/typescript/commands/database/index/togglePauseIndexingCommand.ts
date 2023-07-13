import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";
import genUtils = require("common/generalUtils");

type optsNames = {
    name: string;
}

type toggleType = "map" | "map-reduce";

type optsType = {
    type: toggleType;
}

class togglePauseIndexingCommand extends commandBase {

    private toggleAll = false;
    private readonly name: string;
    private readonly type: toggleType;
    private readonly location: databaseLocationSpecifier;

    private readonly start: boolean;

    private readonly db: database | DatabaseSharedInfo;

    constructor(start: boolean, db: database | DatabaseSharedInfo, options: optsNames | optsType = null, location: databaseLocationSpecifier = undefined) {
        super();
        this.db = db;
        this.start = start;

        this.location = location;
        
        if (options && "name" in options) {
            this.name = options.name;
        } else if (options && "type" in options) {
            this.type = options.type;
        } else {
            this.toggleAll = true;
        }
    }

    execute(): JQueryPromise<void> {
        const basicUrl = this.start ? endpoints.databases.adminIndex.adminIndexesStart : endpoints.databases.adminIndex.adminIndexesStop;

        const args: any = {
            ...this.location
        };
        if (this.name) {
            args.name = this.name;
        } else if (this.type) {
            args.type = this.type;
        }

        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(this.getFailTitle(), response.responseText));
    }

    getFailTitle() {
        const locationText = genUtils.formatLocation(this.location);
        const failText = `Failed to ${this.start ? "resume" : "pause"} ${this.name ? `index ${this.name}` : "all indexes"} for ${locationText}`;

        return failText;
    }
}

export = togglePauseIndexingCommand;
