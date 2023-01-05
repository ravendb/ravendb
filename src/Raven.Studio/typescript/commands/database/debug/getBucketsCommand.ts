import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getBucketsCommand extends commandBase {
    
    static expectedNumberOfRanges = 32;
    
    constructor(private db: database, private range?: { from: number, to: number }) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.BucketsResults> {
        const args = this.range ? {
            fromBucket: this.range.from,
            toBucket: this.range.to,
            range: (this.range.to - this.range.from) / getBucketsCommand.expectedNumberOfRanges
        } : {};
        const url = endpoints.databases.buckets.debugBuckets + this.urlEncodeArgs(args);
        return this.query<Raven.Server.Web.Studio.Processors.BucketsResults>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to buckets report", response.responseText, response.statusText));
    }
}

export = getBucketsCommand;
