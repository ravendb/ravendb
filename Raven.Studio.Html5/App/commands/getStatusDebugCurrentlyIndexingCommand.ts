import commandBase = require("commands/commandBase");
import database = require("models/database");

class getStatusDebugCurrentlyIndexingCommand extends commandBase {

    constructor(private db: database, private withReduction: boolean = false) {
        super();
    }

    execute(): JQueryPromise<statusDebugCurrentlyIndexingDto> {
		var url = "/debug/currently-indexing";
		if (this.withReduction) url+= "?&with_reduction=true";
        return this.query<statusDebugCurrentlyIndexingDto>(url, null, this.db);
    }
}

export = getStatusDebugCurrentlyIndexingCommand;