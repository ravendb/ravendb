import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class forceLicenseUpdate extends commandBase {

    execute(): JQueryPromise<boolean> {
        var url = "/admin/license/forceUpdate";
        return this.query(url, null, appUrl.getSystemDatabase(), r => r.Success);
    }

}

export = forceLicenseUpdate;
