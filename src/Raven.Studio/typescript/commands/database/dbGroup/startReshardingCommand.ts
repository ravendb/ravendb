import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class startReshardingCommand extends commandBase {
    constructor(private db: database, private range: { from: number, to: number }, private targetShard: number) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            bucket: [this.range.from, this.range.to],
            to: this.targetShard,
            database: this.db.name
        }
     
        const url = endpoints.global.resharding.adminReshardingStart + this.urlEncodeArgs(args);
        return this.query<operationIdDto>(url, null, undefined)
            .fail((response: JQueryXHR) => this.reportError("Failed to start resharding operation", response.responseText, response.statusText));
    }
}

export = startReshardingCommand;
