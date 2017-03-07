import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class getDocumentsPreviewCommand extends commandBase {

    static readonly ObjectStubsKey = "$o";
    static readonly ArrayStubsKey = "$a";
    static readonly TrimmedValueKey = "$t";

    constructor(private database: database, private skip: number, private take: number, private collectionName?: string, private bindings?: string[]) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        const args = {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.bindings
        };

        const resultsSelector = (dto: resultsWithTotalCountDto<documentDto>, xhr: JQueryXHR) => {
            return {
                items: dto.Results.map(x => this.mapToDocument(x)),
                totalResultCount: dto.TotalResults,
                resultEtag: this.extractEtag(xhr)
            } as pagedResult<document>;
        };
        const url = endpoints.databases.studioCollections.studioCollectionsPreview;
        return this.query(url, args, this.database, resultsSelector);
    }

    private mapToDocument(docDto: documentDto) {
        const doc = new document(docDto);

        const metadata = doc.__metadata;

        const objectStubs = (metadata as any)[getDocumentsPreviewCommand.ObjectStubsKey] as string[];
        if (objectStubs) {
            objectStubs.forEach(stub => (doc as any)[stub] = {});
        }

        const arrayStubs = (metadata as any)[getDocumentsPreviewCommand.ArrayStubsKey] as string[];
        if (arrayStubs) {
            arrayStubs.forEach(stub => (doc as any)[stub] = []);
        }

        const trimmedValues = (metadata as any)[getDocumentsPreviewCommand.TrimmedValueKey] as string[];
        if (trimmedValues) {
            trimmedValues.forEach(trimmedKey => {
                (doc as any)[trimmedKey] += "...";
            });
        }

        return doc;
    }

}

export = getDocumentsPreviewCommand;
