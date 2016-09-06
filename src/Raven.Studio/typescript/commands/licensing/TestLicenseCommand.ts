import commandBase = require("commands/commandBase");

class TestLicenseCommand extends commandBase {

    execute(): JQueryPromise<any> {
        return this.query("/admin/test-hotspare", null, null, null);//TODO: use endpoints
    }
}

export = TestLicenseCommand; 
