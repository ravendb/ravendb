import commandBase = require("commands/commandBase");
import database = require("models/database");

class getIndexingStatusCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        var result = $.Deferred();

        var url = "/admin/indexingStatus";
        this.query(url, null, this.db)
            .done((indexingStatus) => result.resolve(indexingStatus))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to create backup!", response.responseText, response.statusText);
                result.reject();
            });
        return result;
    }

}

export = getIndexingStatusCommand;
