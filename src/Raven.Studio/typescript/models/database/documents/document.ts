import documentMetadata = require("models/database/documents/documentMetadata");

class document implements documentBase {
    __metadata: documentMetadata;

    constructor(dto: documentDto) {
        this.__metadata = new documentMetadata(dto["@metadata"]);
        for (var property in dto) {
            if (property !== "@metadata") {
                (<any>this)[property] = dto[property];
            }
        }
    }

    getCollection() {
        return this.__metadata.collection;
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

    toBulkDoc(method: Raven.Client.Documents.Commands.Batches.CommandType): Raven.Server.Documents.Handlers.BatchRequestParser.CommandData {
        const dto = this.toDto(true);
        const bulkDoc = {
            Document: dto,
            Key: this.getId(),
            Type: method
        } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData;

        const meta = dto["@metadata"];
        if (meta) {
            //TODO: bulkDoc.Metadata = meta;

            if (meta["@etag"]) {
                bulkDoc.Etag = meta["@etag"];
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

    static getCollectionFromId(id: string): string {
        if (!id) {
            return null;
        }

        const slashIndex = id.lastIndexOf("/");
        if (slashIndex >= 1) {
            return id.substring(0, 1).toUpperCase() + id.substring(1, slashIndex);
        }

        return id;
    }
}

export = document;