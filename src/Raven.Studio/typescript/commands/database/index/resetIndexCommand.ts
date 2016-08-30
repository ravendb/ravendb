import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class resetIndexCommand extends commandBase {

    constructor(private indexNameToReset: string, private db:database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/indexes/" + this.indexNameToReset;//TODO: use endpoints
        return this.reset(url, null, this.db)
            .done(() => this.reportSuccess("Index " + this.indexNameToReset + " successfully reset"))
            .fail((response: JQueryXHR) => this.reportError("Failed to reset index: " + this.indexNameToReset, response.responseText, response.statusText));
    }

}


export = resetIndexCommand;
