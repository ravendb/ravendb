import commandBase = require("commands/commandBase");
import database = require("models/database");

class getValidateWebSocketParamsCommand extends commandBase {

    constructor(private db: database, private connectionString: string) {
        super();
    }

    execute(): JQueryPromise<taskMetadataDto[]> {
        var url = "/debug/websocket?" + this.connectionString;
        return this.query<any>(url, null, this.db);
    }
}

export = getValidateWebSocketParamsCommand;