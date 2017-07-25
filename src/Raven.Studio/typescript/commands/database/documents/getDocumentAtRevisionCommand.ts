import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getDocumentAtRevisionCommand extends commandBase {

    constructor(private changeVector: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<document> {
        const args = {
            changeVector: this.changeVector
        }

        const url = endpoints.databases.revisions.revisions + this.urlEncodeArgs(args);

        return this.query(url, null, this.db, (results: resultsDto<documentDto>) => {
            return results.Results.map(x => new document(x))[0];
        });
    }
 }

export = getDocumentAtRevisionCommand;
