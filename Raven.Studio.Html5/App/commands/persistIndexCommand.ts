import commandBase = require("commands/commandBase");
import database = require("models/database");
import index = require("models/index");
import indexPriority = require("models/indexPriority");

class persistIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/indexes/" + this.indexName + "?op=forceWriteToDisk"; 
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess("Successfully persisted index: " + this.indexName))
            .fail((response: JQueryXHR) => this.reportError("Error persisting index", response.responseText, response.statusText));
    }
}

export = persistIndexCommand; 