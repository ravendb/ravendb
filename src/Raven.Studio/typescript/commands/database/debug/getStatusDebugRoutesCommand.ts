import commandBase = require("commands/commandBase");

class getStatusDebugConfigCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/debug/routes";//TODO: use endpoints
        return this.query<any>(url, null, null);
    }
}

export = getStatusDebugConfigCommand;
