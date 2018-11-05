import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

//TODO: make sure we support parameters
class debugGraphOutputCommand extends commandBase {
    constructor(private db: database, private queryText: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.query(this.getUrl(), null, this.db)
            .fail((response: JQueryXHR) => {
                if (response.status === 404) {
                    this.reportError("Error querying index", "Index was not found", response.statusText)
                } else {
                    this.reportError("Error querying index", response.responseText, response.statusText)
                }
            });
    }

    getUrl() {
        const url = endpoints.databases.queries.queries;
        
        const urlArgs = this.urlEncodeArgs({
            query: this.queryText,
            start: 0,
            pageSize: 128, //TODO: make this customizable 
            debug: "graph"
        });
        return url + urlArgs;
    }
}

export = debugGraphOutputCommand;
