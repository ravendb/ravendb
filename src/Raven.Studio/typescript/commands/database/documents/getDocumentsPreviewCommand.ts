import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class stubsObjectsContainer {
    // eslint-disable-next-line @typescript-eslint/ban-types
    private static stubsObjects: dictionary<{}> = {};
  
    static getStubObject(propertiesCount: number) {
        if (propertiesCount in stubsObjectsContainer.stubsObjects) {
            return stubsObjectsContainer.stubsObjects[propertiesCount];
        } else {
            const stubObject: Record<string, number> = {};

            for (let i = 0; i < propertiesCount; i++) {
                stubObject["$$fake$$" + i] = null;
            }

            stubsObjectsContainer.stubsObjects[propertiesCount] = stubObject;
            return stubObject;
        } 
    }
}

class getDocumentsPreviewCommand extends commandBase {

    static readonly ObjectStubsKey = "$o";
    static readonly ArrayStubsKey = "$a";
    static readonly TrimmedValueKey = "$t";

    constructor(private database: database | string, private skip: number, private take: number, private collectionName: string,
                private previewBindings?: string[], private fullBindings?: string[], private continuationToken?: string) {
        super();
    }

    execute(): JQueryPromise<pagedResultWithAvailableColumns<document>> {
        const resultsSelector = (dto: resultsWithCountAndAvailableColumns<documentDto>, xhr: JQueryXHR): pagedResultWithAvailableColumns<document> => {
            dto.AvailableColumns.push("__metadata");
            return {
                items: dto.Results.map(x => this.mapToDocument(x)),
                totalResultCount: dto.TotalResults,
                resultEtag: this.extractEtag(xhr),
                availableColumns: dto.AvailableColumns,
                continuationToken: dto.ContinuationToken
            };
        };

        const args = this.getArgsToUse();
        const url = endpoints.databases.studioCollections.studioCollectionsPreview + this.urlEncodeArgs(args);
        
        return this.query(url, null, this.database, resultsSelector);
    }

    private mapToDocument(docDto: documentDto) {
        const doc = new document(docDto);

        const metadata = doc.__metadata as any;

        const objectStubs = metadata[getDocumentsPreviewCommand.ObjectStubsKey] as System.Collections.Generic.Dictionary<string, number>;
        if (objectStubs) {
            Object.keys(objectStubs).forEach(stubKey => (doc as any)[stubKey] =
                stubsObjectsContainer.getStubObject(objectStubs[stubKey]));
        }

        const arrayStubs = metadata[getDocumentsPreviewCommand.ArrayStubsKey] as System.Collections.Generic.Dictionary<string, number>;
        if (arrayStubs) {
            Object.keys(arrayStubs).forEach(stubKey => (doc as any)[stubKey] =
                new Array(arrayStubs[stubKey])); 
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
    
    private getArgsToUse() {
        if (this.continuationToken) {
            return {
                collection: this.collectionName,
                binding: this.previewBindings,
                fullBinding: this.fullBindings,
                continuationToken: this.continuationToken
            };
        }

        return {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.previewBindings,
            fullBinding: this.fullBindings
        };
    }
}

export = getDocumentsPreviewCommand;
