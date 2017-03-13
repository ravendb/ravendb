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

    execute(): JQueryPromise<pagedResultWithAvailableColumns<document>> {
        const args = {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.bindings
        };

        const resultsSelector = (dto: resultsWithCountAndAvailableColumns<documentDto>, xhr: JQueryXHR) => {

            dto.AvailableColumns.push("__metadata");

            return {
                items: dto.Results.map(x => this.mapToDocument(x)),
                totalResultCount: dto.TotalResults,
                resultEtag: this.extractEtag(xhr),
                availableColumns: dto.AvailableColumns
            } as pagedResultWithAvailableColumns<document>;
        };
        const url = endpoints.databases.studioCollections.studioCollectionsPreview + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, resultsSelector);
    }

    private mapToDocument(docDto: documentDto) {
        const doc = new document(docDto);

        const metadata = doc.__metadata as any;

        const objectStubs = metadata[getDocumentsPreviewCommand.ObjectStubsKey] as string[];
        if (objectStubs) {
            objectStubs.forEach(stub => (doc as any)[stub] = {});
        }

        const arrayStubs = metadata[getDocumentsPreviewCommand.ArrayStubsKey] as string[];
        if (arrayStubs) {
            arrayStubs.forEach(stub => (doc as any)[stub] = []);
        }

        const trimmedValues = metadata[getDocumentsPreviewCommand.TrimmedValueKey] as string[];
        if (trimmedValues) {
            trimmedValues.forEach(trimmedKey => {
                (doc as any)[trimmedKey] += "...";
            });
        }

        // we don't delete $o, $a, $t from document as it is used to detect if we display fake value

        return doc;
    }

}

export = getDocumentsPreviewCommand;
