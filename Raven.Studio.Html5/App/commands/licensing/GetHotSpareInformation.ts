import commandBase = require("commands/commandBase");

class GetHotSpareInformation extends commandBase {

    execute(): JQueryPromise<HotSpareDto> {
        var q = this.query("/admin/get-hotspare-information", null, null, null);
        q.fail((response: JQueryXHR) => this.reportError("Can't fetch Hot Spare license information", response.responseText, response.statusText));
        return q;
    }
}

export = GetHotSpareInformation; 
