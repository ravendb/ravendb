import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveBulkOfDocuments extends commandBase {

    constructor(private dataTypes:string, private bulkDocuments: Array<bulkDocumentDto>, private db: database) {
        super();
    }

    // performs bulk save of an array of objects 
    execute(): JQueryPromise<any> {
        
        var saveBulkTask = this.post(endpoints.databases.batch.bulk_docs, ko.toJSON(this.bulkDocuments), this.db);
        this.reportInfo("Performing bulk save of " + this.dataTypes);

        saveBulkTask.done(() => this.reportSuccess("Saved all bulk of documents"));
        saveBulkTask.fail((response: JQueryXHR) => this.reportError("Failed to save bulk of documents", response.responseText, response.statusText));

        return saveBulkTask;
    }
}

export = saveBulkOfDocuments;
