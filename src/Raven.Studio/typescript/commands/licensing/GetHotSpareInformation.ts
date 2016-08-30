import commandBase = require("commands/commandBase");

class GetHotSpareInformation extends commandBase {

    execute(): JQueryPromise<HotSpareDto> {
        return this.query("/admin/get-hotspare-information", null, null, null);//TODO: use endpoints
    }
}

export = GetHotSpareInformation; 
