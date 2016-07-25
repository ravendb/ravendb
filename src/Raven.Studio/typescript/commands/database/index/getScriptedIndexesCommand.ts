import commandBase = require("commands/commandBase");
import scriptedIndex = require("models/database/index/scriptedIndex");
import database = require("models/resources/database");

class getScriptedIndexesCommand extends commandBase {

    constructor(private db: database, private indexName = "") {
        super();
    }

    execute(): JQueryPromise<Array<scriptedIndex>> {
        var args = {
            startsWith: "Raven/ScriptedIndexResults/" + this.indexName,
            exclude: <string>null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, this.db, (dtos: scriptedIndexDto[]) => dtos.map(dto => new scriptedIndex(dto)));
    }
}

export = getScriptedIndexesCommand;
