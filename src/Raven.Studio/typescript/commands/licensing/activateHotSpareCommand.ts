import commandBase = require("commands/commandBase");

class activateHotSpareCommand extends commandBase {

    execute(): JQueryPromise<any> {
        return this.query("/admin/activate-hotspare", null, null, null, { dataType: undefined })
            .done(() => this.reportSuccess("Activated Hot Spare mode"))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to activate Hot Spare mode!", response.responseText, response.statusText);
            });
    }
}

export = activateHotSpareCommand; 
