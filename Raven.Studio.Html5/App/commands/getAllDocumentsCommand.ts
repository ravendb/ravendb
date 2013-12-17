import commandBase = require("commands/commandBase");
import database = require("models/database");
import pagedResultSet = require("common/pagedResultSet");
import document = require("models/document");

/*
 * getAllDocumentsCommand is a specialized command that fetches all the documents in a specified database.
*/
class getAllDocumentsCommand extends commandBase {

    constructor(private ownerDatabase: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {

        // Getting all documents requires a 2 step process:
        // 1. Fetch /indexes/Raven/DocumentsByEntityName to get the total doc count.
        // 2. Fetch /docs to get the actual documents.

        // Fetching #1 will return a document list, but it won't include the system docs.
        // Therefore, we must fetch /docs as well, which gives us the system docs.

        var docsTask = this.fetchDocs();
        var totalResultsTask = this.fetchTotalResultCount();
        var doneTask = $.Deferred();
        var combinedTask = $.when(docsTask, totalResultsTask);
        combinedTask.done((docsResult: document[], resultsCount: number) => doneTask.resolve(new pagedResultSet(docsResult, resultsCount)));
        combinedTask.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }

    private fetchDocs(): JQueryPromise<document[]> {
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var docSelector = (docs: documentDto[]) => docs.map(d => new document(d));
        return this.query("/docs", args, this.ownerDatabase, docSelector);
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        var args = {
            query: "",
            start: 0,
            pageSize: 0
        };

        var url = "/indexes/Raven/DocumentsByEntityName";
        var countSelector = (dto: collectionInfoDto) => dto.TotalResults;
        return this.query(url, args, this.ownerDatabase, countSelector);
    }
}

export = getAllDocumentsCommand;