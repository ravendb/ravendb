import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testIndexCommand extends commandBase {

    constructor(private testRequest: Raven.Server.Documents.Indexes.Test.TestIndexParameters, private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.Test.TestIndexResult> {
        const url = endpoints.databases.adminIndex.indexesTest + this.urlEncodeArgs(this.location);
        
        return this.post(url, JSON.stringify(this.testRequest), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to test index", response.responseText, response.statusText);
            });
    }
}

export = testIndexCommand; 
