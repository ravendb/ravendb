import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getBucketCommand extends commandBase {
    
    constructor(private db: database, private bucket: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.BucketInfo> {
        const args = {
            bucket: this.bucket
        }
        const url = endpoints.databases.buckets.debugBucket + this.urlEncodeArgs(args);
        return this.query<Raven.Server.Web.Studio.Processors.BucketInfo>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to load bucket", response.responseText, response.statusText));
    }
}

export = getBucketCommand;
