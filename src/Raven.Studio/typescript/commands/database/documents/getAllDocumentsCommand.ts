import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

/*
 * getAllDocumentsCommand is a specialized command that fetches all the documents in a specified database.
*/
class getAllDocumentsCommand extends commandBase {

    constructor(private ownerDatabase: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<any>> {

        // Getting all documents requires a 2 step process:
        // 1. Fetch /collections/stats to get the total doc count.
        // 2. Fetch /docs to get the actual documents.

        var docsTask = this.fetchDocs();
        var totalResultsTask = this.fetchTotalResultCount();
        var doneTask = $.Deferred();
        var combinedTask = $.when<any>(docsTask, totalResultsTask);
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
        var url = endpoints.databases.collections.collectionsStats;
        var countSelector = (dto: collectionsStatsDto) => dto.NumberOfDocuments;
        return this.query(url, null, this.ownerDatabase, countSelector);
    }
}

export = getAllDocumentsCommand;
