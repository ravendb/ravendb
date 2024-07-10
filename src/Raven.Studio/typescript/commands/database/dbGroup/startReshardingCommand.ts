import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

interface Range {
    from: number;
    to: number;
}

class startReshardingCommand extends commandBase {
    private readonly databaseName: string;
    private readonly range: Range;
    private readonly targetShard: number;

    constructor(db: database | string,  range: Range, targetShard: number) {
        super();
        this.databaseName = typeof db === "string" ? db : db.name;
        this.range = range;
        this.targetShard = targetShard;
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            fromBucket: this.range.from,
            toBucket: this.range.to,
            toShard: this.targetShard,
            database: this.databaseName
        }
     
        const url = endpoints.global.resharding.adminReshardingStart + this.urlEncodeArgs(args);
        return this.post<operationIdDto>(url, null, undefined)
            .fail((response: JQueryXHR) => this.reportError("Failed to start resharding operation", response.responseText, response.statusText));
    }
}

export = startReshardingCommand;
