import commandBase = require("commands/commandBase");
import scriptedIndex = require("models/scriptedIndex");
import database = require("models/database");

class getScriptedIndexesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<scriptedIndex>> {
        var args = {
            startsWith: "Raven/ScriptedIndexResults/",
            exclude: null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, this.db, (dtos: scriptedIndexDto[]) => dtos.map(dto => new scriptedIndex(dto)));
    }
}

export = getScriptedIndexesCommand;