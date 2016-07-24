import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class licenseCheckConnectivityCommand extends commandBase {

    execute(): JQueryPromise<boolean> {
        var url = "/admin/license/connectivity";
        return this.query(url, null, null, r => r.Success);
    }

}

export = licenseCheckConnectivityCommand; 
