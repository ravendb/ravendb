import commandBase = require("commands/commandBase");

class forceLicenseUpdate extends commandBase {

    execute(): JQueryPromise<boolean> {
        let url = "/admin/license/forceUpdate"; //TODO: use endpoints
        return this.query(url, null, null, r => r.Success);
    }

}

export = forceLicenseUpdate;
