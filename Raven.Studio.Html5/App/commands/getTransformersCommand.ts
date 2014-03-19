/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");

class getTransformersCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<transformerDto[]> {
        var args = {
            start: this.skip,
            pageSize: this.take
        };
        var url = "/transformers" + this.urlEncodeArgs(args);
        return this.query(url, null, this.db);
    }
}

export = getTransformersCommand;