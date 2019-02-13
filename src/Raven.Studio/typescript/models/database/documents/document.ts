import documentMetadata = require("models/database/documents/documentMetadata");

class document implements documentBase {
    __metadata: documentMetadata;

    constructor(dto: documentDto) {
        this.__metadata = new documentMetadata(dto["@metadata"]);
        for (let property in dto) {
            if (property !== "@metadata") {
                (<any>this)[property] = dto[property];
            }
        }
    }

    getCollection() {
        return this.__metadata.collection || "@empty";
    }

    getId() {
        return this.__metadata.id;
    }

    getUrl() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        const propertyNames: Array<string> = [];
        for (let property in this) {
            const isMeta = property === "__metadata" || property === "__moduleId__";
            const isFunction = _.isFunction((this as any)[property]);
            if (!isMeta && !isFunction) {
                propertyNames.push(property);
            }
        }

        return propertyNames;
    }

    toDto(includeMeta: boolean = false): documentDto {
        const dto: any = { };
        const properties = this.getDocumentPropertyNames();
        for (let i = 0; i < properties.length; i++) {
            let property = properties[i];
            dto[property] = (<any>this)[property];
        }

        dto["@metadata"] = includeMeta && this.__metadata ? this.__metadata.toDto() : undefined;

        return dto;
    }

    /**
     * serialize document to dto but unify metadata: put counters under common key regardless if document is revision or no
     */
    toDiffDto() {
        const dto = this.toDto(true);
        
        if (this.__metadata && this.__metadata.revisionCounters) {
            dto["@metadata"]["@counters"] = this.__metadata.revisionCounters().map(x => x.name);
        }
        
        return dto;
    }

    toBulkDoc(method: Raven.Client.Documents.Commands.Batches.CommandType): Raven.Server.Documents.Handlers.BatchRequestParser.CommandData {
        const dto = this.toDto(true);
        const bulkDoc = {
            Document: dto,
            Id: this.getId(),
            Type: method
        } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData;

        const meta = dto["@metadata"];
        if (meta) {
            if (meta["@change-vector"]) {
                bulkDoc.ChangeVector = meta["@change-vector"];
            }
        }

        return bulkDoc;
    }

    static empty(): document {
        const emptyDto = {
            '@metadata': {}
        };

        return new document(<any>emptyDto);
    }

    static getCollectionFromId(id: string, collections: string[]): string {
        if (!id) {
            return null;
        }

        // Get first index of '/' or '|'. Otherwise return -1;
        const indexes = [id.indexOf("/"), id.indexOf("|")].filter(x => x !== -1);
        const firstSeparatorIndex = _.min(indexes.length ? indexes : [-1]);

        if (firstSeparatorIndex >= 1) {
            let collectionName = id.substring(0, firstSeparatorIndex);
            
            if (collectionName.toLocaleLowerCase() === collectionName) {
                // All letters are lower case, Capitalize first 
                collectionName = _.capitalize(collectionName);
            } else {
                // Find an already existing matching collection name 
                collectionName = collections.find(collection => collection.toLocaleLowerCase() === collectionName.toLocaleLowerCase());
            }
        
             return collectionName;
        }

        // if no '/' or '|' at all then we want the document to be in the @empty collection
        return null;
    }
}

export = document;
