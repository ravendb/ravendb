import database = require("models/resources/database");
import commandBase = require("commands/commandBase");

class getServerPrefixForHiLoCommand extends commandBase {

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<string> {
        var resultsSelector = (queryResult: any) => queryResult.ServerPrefix;
        var url = "/document?id=Raven/ServerPrefixForHilo";
        return this.query(url, null, this.db, resultsSelector);
    }
}

export = getServerPrefixForHiLoCommand;
