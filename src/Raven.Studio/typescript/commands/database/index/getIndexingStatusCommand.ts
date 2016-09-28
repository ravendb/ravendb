import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexingStatusCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        var result = $.Deferred();

        var url = "/admin/indexingStatus";//TODO: use endpoints
        this.query(url, null, this.db)
            .done((indexingStatus) => result.resolve(indexingStatus))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get indexing status!", response.responseText, response.statusText);
                result.reject();
            });
        return result;
    }

}

export = getIndexingStatusCommand;
