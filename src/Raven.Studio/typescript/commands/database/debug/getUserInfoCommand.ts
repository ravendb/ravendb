import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getUserInfoCommand extends commandBase {

    constructor(private db: database = null) {
        super();
    }

    execute(): JQueryPromise<userInfoDto> {
        var url = "/debug/user-info";//TODO: use endpoints
        return this.query<userInfoDto>(url, null, this.db);
    }
}

export = getUserInfoCommand;
