import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

type docsAndEtag = {
    docs: document[],
    etag: string
}

class getAllDocumentsMetadataCommand extends commandBase {

    constructor(private ownerDatabase: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        // Getting all documents requires a 2 step process:
        // 1. Fetch /collections/stats to get the total doc count.
        // 2. Fetch /docs to get the actual documents.

        //TODO: fill result etag
        const docsTask = this.fetchDocs();
        const totalResultsTask = this.fetchTotalResultCount();
        const doneTask = $.Deferred<pagedResult<document>>();

        $.when<any>(docsTask, totalResultsTask)
            .done(([result]: [docsAndEtag], [resultsCount]: [number]) => {
                doneTask.resolve({ items: result.docs, totalResultCount: resultsCount, resultEtag: result.etag })
            })
            .fail(xhr => doneTask.reject(xhr));

        return doneTask;
    }

    private fetchDocs(): JQueryPromise<docsAndEtag> {
        const args = {
            start: this.skip,
            pageSize: this.take,
            'metadata-only': true
        };

        const docSelector = (docs: resultsDto<documentDto>, xhr: JQueryXHR) => {
            return { docs: docs.Results.map(d => new document(d)), etag: this.extractEtag(xhr) } as docsAndEtag;
        };
        const url = endpoints.databases.document.docs;

        return this.query(url, args, this.ownerDatabase, docSelector);
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        const url = endpoints.databases.collections.collectionsStats;
        const countSelector = (dto: Raven.Client.Documents.Operations.CollectionStatistics) => dto.CountOfDocuments;
        return this.query(url, null, this.ownerDatabase, countSelector);
    }
}

export = getAllDocumentsMetadataCommand;
