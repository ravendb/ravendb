import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexNamesCommand extends commandBase {
    constructor(private db: database, private withType = false) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.withType ? "namesWithTypeOnly" : "namesOnly";
        var url = "/indexes/?" + action + "=true";
        return this.query(url, null, this.db);
    }
}

export = getIndexNamesCommand;
