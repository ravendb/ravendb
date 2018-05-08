import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCountersCommand extends commandBase {

    constructor(private docId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Counters.CountersDetail> {
        const args = {
            docId: this.docId,
            full: true
        };

        const url = endpoints.databases.counters.counters;

        return this.query<Raven.Client.Documents.Operations.Counters.CountersDetail>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((result: JQueryXHR) => this.reportError("Failed to get counter details", result.responseText, result.statusText));
    }
 }

export = getCountersCommand; 
