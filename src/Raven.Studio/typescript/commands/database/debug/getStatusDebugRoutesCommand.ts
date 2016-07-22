import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class getStatusDebugConfigCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/debug/routes";
        return this.query<any>(url, null, null);
    }
}

export = getStatusDebugConfigCommand;
