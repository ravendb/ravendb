import commandBase = require("commands/commandBase");

class testHotSpareCommand extends commandBase {

    execute(): JQueryPromise<any> {
        return this.query("/admin/test-hotspare", null, null, null, { dataType: undefined })//TODO: use endpoints
            .done(() => this.reportSuccess("Activated Hot Spare test mode"))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to enable Hot Spare test mode!", response.responseText, response.statusText);
            });
    }
}

export = testHotSpareCommand; 
