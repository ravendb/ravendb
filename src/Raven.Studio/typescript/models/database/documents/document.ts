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

    getEntityName() {
        return this.__metadata.ravenEntityName;
    }

    getId() {
        return this.__metadata.id;
    }

    getUrl() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        var propertyNames: Array<string> = [];
        for (var property in this) {
            var isMeta = property === "__metadata" || property === "__moduleId__";
            var isFunction = typeof (<any>this)[property] === "function";
            if (!isMeta && !isFunction) {
                propertyNames.push(property);
            }
        }

        return propertyNames;
    }

    toDto(includeMeta: boolean = false): documentDto {
        var dto: any = { };
        var properties = this.getDocumentPropertyNames();
        for (var i = 0; i < properties.length; i++) {
            var property = properties[i];
            dto[property] = (<any>this)[property];
        }

        dto["@metadata"] = includeMeta && this.__metadata ? this.__metadata.toDto() : undefined;

        return dto;
    }

    toBulkDoc(method: string): bulkDocumentDto {
        var dto = this.toDto(true);
        var bulkDoc: bulkDocumentDto = {
            Document: dto,
            Key: this.getId(),
            Method: method,
            AdditionalData: null
        };

        var meta = dto["@metadata"];
        if (meta) {
            bulkDoc.Metadata = meta;

            if (meta["@etag"]) {
                bulkDoc.Etag = meta["@etag"];
            }
        }

        return bulkDoc;
    }

    static empty(): document {
        var emptyDto = {
            '@metadata': {}
        };

        return new document(<any>emptyDto);
    }

    static getEntityNameFromId(id: string): string {
        if (!id) {
            return null;
        }

        var slashIndex = id.lastIndexOf("/");
        if (slashIndex >= 1) {
            return id.substring(0, 1).toUpperCase() + id.substring(1, slashIndex);
        }

        return id;
    }
}

export = document;