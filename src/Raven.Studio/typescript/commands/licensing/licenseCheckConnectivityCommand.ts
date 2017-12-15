import commandBase = require("commands/commandBase");

class licenseCheckConnectivityCommand extends commandBase {

    execute(): JQueryPromise<boolean> {
        const url = "/admin/license/connectivity";//TODO: use endpoints
        return this.query(url, null, null, r => r.Success);
    }

}

export = licenseCheckConnectivityCommand; 
