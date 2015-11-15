import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class licenseCheckConnectivityCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<boolean> {
        var url = "/admin/license/connectivity";
        return this.query(url, null, appUrl.getSystemDatabase(), r => r.Success);
    }

}

export = licenseCheckConnectivityCommand; 
