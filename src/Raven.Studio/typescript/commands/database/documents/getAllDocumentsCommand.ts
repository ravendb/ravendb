import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getAllDocumentsCommand extends commandBase {

    constructor(private ownerDatabase: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<document>> {
        // Getting all documents requires a 2 step process:
        // 1. Fetch /collections/stats to get the total doc count.
        // 2. Fetch /docs to get the actual documents.

        const docsTask = this.fetchDocs();
        const totalResultsTask = this.fetchTotalResultCount();
        const doneTask = $.Deferred<pagedResultSet<document>>();

        $.when<any>(docsTask, totalResultsTask)
            .done(([docsResult]: [document[]], [resultsCount]: [number]) => doneTask.resolve(new pagedResultSet(docsResult, resultsCount)))
            .fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchDocs(): JQueryPromise<document[]> {
        const args = {
            start: this.skip,
            pageSize: this.take
        };

        const docSelector = (docs: resultsDto<documentDto>) => docs.Results.map(d => new document(d));
        const url = endpoints.databases.document.docs;

        return this.query(url, args, this.ownerDatabase, docSelector);
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        const url = endpoints.databases.collections.collectionsStats;
        const countSelector = (dto: Raven.Client.Documents.Operations.CollectionStatistics) => dto.CountOfDocuments;
        return this.query(url, null, this.ownerDatabase, countSelector);
    }
}

export = getAllDocumentsCommand;
