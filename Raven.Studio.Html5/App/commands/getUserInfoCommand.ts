import commandBase = require("commands/commandBase");
import database = require("models/database");

class getUserInfoCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<userInfoDto> {
        var url = "/debug/user-info";
        return this.query<userInfoDto>(url, null, this.db);
    }
}

export = getUserInfoCommand;