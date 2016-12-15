import commandBase = require("commands/commandBase");

//TODO: rename to lower case
class testLicenseCommand extends commandBase {

    execute(): JQueryPromise<any> {
        return this.query("/admin/test-hotspare", null, null, null);//TODO: use endpoints
    }
}

export = testLicenseCommand; 
