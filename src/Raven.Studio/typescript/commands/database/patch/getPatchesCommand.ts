import commandBase = require("commands/commandBase");
import patchDocument = require("models/database/patch/patchDocument");
import database = require("models/resources/database");

class getPatchesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<patchDocument>> {
        var args = {
            startsWith: "Studio/Patch/",
            exclude: <string>null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, this.db, (dtos: patchDto[]) => dtos.map(dto => new patchDocument(dto)));//TODO: use endpoints
    }
}

export = getPatchesCommand;
