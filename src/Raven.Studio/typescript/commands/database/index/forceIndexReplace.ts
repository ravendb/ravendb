import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class forceIndexReplace extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Replacing index (forced)");
        var url = "/indexes/" + this.indexName + "?op=forceReplace";//TODO: use endpoints
        return this.post(url, null, this.db, {dataType: undefined}).done(() => {
            this.reportSuccess("Replaced index " + this.indexName);
        }).fail((response: JQueryXHR) => this.reportError("Failed to replace index.", response.responseText, response.statusText));
    }

}

export = forceIndexReplace; 
