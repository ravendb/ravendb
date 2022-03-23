import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils from "common/generalUtils";

class getIndexesErrorCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const url = endpoints.databases.index.indexesErrors + this.urlEncodeArgs(this.location);

        const locationText = genUtils.formatLocation(this.location);
        
        return this.query<Raven.Client.Documents.Indexes.IndexErrors[]>(url, null, this.db, x => x.Results)
            .fail((result: JQueryXHR) => this.reportError(`Failed to get index errors for ${locationText}`, result.responseText, result.statusText));
    }
}

export = getIndexesErrorCommand;
