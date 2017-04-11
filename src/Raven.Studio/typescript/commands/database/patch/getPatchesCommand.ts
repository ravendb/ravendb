import commandBase = require("commands/commandBase");
import patchDocument = require("models/database/patch/patchDocument");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getPatchesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<patchDocument>> {
        const args = {
            startsWith: "Raven/Studio/Patch/",
            exclude: <string>null,
            start: 0,
            pageSize: 1024
        };

        return this.query(endpoints.databases.document.docs, args, this.db,
            (dtos: queryResultDto<patchDto>) => dtos.Results.map(dto => new patchDocument(dto)));
    }
}

export = getPatchesCommand;
