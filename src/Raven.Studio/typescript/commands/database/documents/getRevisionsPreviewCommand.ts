import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface RevisionsPreviewResultItem {
    Id: string;
    Etag: string;
    Collection: string;
    LastModified: string;
    ChangeVector: string;
    Flags: string;
    ShardNumber: number;
}

interface Parameters {
    databaseName: string;
    start: number;
    pageSize: number;
    continuationToken?: string;
    type?: Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;
    collection?: string;
}

export default class getRevisionsPreviewCommand extends commandBase {
    private readonly parameters: Parameters;

    constructor(parameters: Parameters) {
        super();
        this.parameters = parameters;
    }

    execute(): JQueryPromise<pagedResultWithToken<RevisionsPreviewResultItem>> {
        const url = endpoints.databases.studioCollections.studioRevisionsPreview + this.urlEncodeArgs(this.getArgsToUse());

        return this.query(url, null, this.parameters.databaseName, this.resultsSelector).fail((response: JQueryXHR) => {
            this.reportError("Failed to get revisions preview", response.responseText, response.statusText);
        });
    }

    private getArgsToUse() {
        const { start, pageSize, continuationToken, type, collection } = this.parameters;
        
        if (continuationToken) {
            return {
                continuationToken,
                type,
                collection
            };
        }

        return {
            start,
            pageSize,
            type,
            collection
        };
    }

    private resultsSelector(dto: resultsWithCountAndToken<RevisionsPreviewResultItem>): pagedResultWithToken<RevisionsPreviewResultItem> {
        return {
            items: dto.Results,
            totalResultCount: dto.TotalResults,
            continuationToken: dto.ContinuationToken,
        };
    };
}

