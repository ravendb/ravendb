import commandBase = require("commands/commandBase");

//TODO: rename to lower case
class getHotSpareInformation extends commandBase {

    execute(): JQueryPromise<HotSpareDto> {
        return this.query("/admin/get-hotspare-information", null, null, null);//TODO: use endpoints
    }
}

export = getHotSpareInformation; 
