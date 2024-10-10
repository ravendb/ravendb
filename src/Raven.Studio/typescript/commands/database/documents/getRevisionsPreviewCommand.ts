import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface RevisionsPreviewResultItem {
    Id: string;
    Etag: string;
    LastModified: string;
    ChangeVector: string;
    Flags: string;
    ShardNumber: number;
}

export default class getRevisionsPreviewCommand extends commandBase {
    private readonly databaseName: string;
    private readonly start: number;
    private readonly pageSize: number;
    private readonly continuationToken?: string;

    constructor(databaseName: string, start: number, pageSize: number, continuationToken?: string) {
        super();
        this.databaseName = databaseName;
        this.start = start;
        this.pageSize = pageSize;
        this.continuationToken = continuationToken;
    }

    execute(): JQueryPromise<pagedResultWithToken<RevisionsPreviewResultItem>> {
        const url = endpoints.databases.studioCollections.studioRevisionsPreview + this.urlEncodeArgs(this.getArgsToUse());

        return this.query(url, null, this.databaseName, this.resultsSelector).fail((response: JQueryXHR) => {
            this.reportError("Failed to get revisions preview", response.responseText, response.statusText);
        });
    }

    private getArgsToUse() {
        if (this.continuationToken) {
            return {
                continuationToken: this.continuationToken
            };
        }

        return {
            start: this.start,
            pageSize: this.pageSize
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

