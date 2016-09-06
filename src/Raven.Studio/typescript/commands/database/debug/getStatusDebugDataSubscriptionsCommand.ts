import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugDataSubscriptionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<statusDebugDataSubscriptionsDto>> {
        var url = "/debug/subscriptions";//TODO: use endpoints
        return this.query<Array<statusDebugDataSubscriptionsDto>>(url, null, this.db);  
    }
}

export = getStatusDebugDataSubscriptionsCommand;
