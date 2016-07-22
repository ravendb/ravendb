import documentMetadata = require("models/database/documents/documentMetadata");

class document implements documentBase {
    __metadata: documentMetadata;

    constructor(dto: documentDto) {
        this.__metadata = new documentMetadata(dto["@metadata"]);
        for (var property in dto) {
            if (property !== "@metadata") {
                this[property] = dto[property];
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
        var propertyNames = [];
        for (var property in this) {
            var isMeta = property === "__metadata" || property === "__moduleId__";
            var isFunction = typeof this[property] === "function";
            if (!isMeta && !isFunction) {
                propertyNames.push(property);
            }
        }

        return propertyNames;
    }

    toDto(includeMeta: boolean = false): documentDto {
        var dto = { '@metadata': undefined };
        var properties = this.getDocumentPropertyNames();
        for (var i = 0; i < properties.length; i++) {
            var property = properties[i];
            dto[property] = this[property];
        }

        if (includeMeta && this.__metadata) {
            dto["@metadata"] = this.__metadata.toDto();
        }

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

    public static empty(): document {
        var emptyDto = {
            '@metadata': {}
        };

        return new document(<any>emptyDto);
    }

    public static getEntityNameFromId(id: string): string {
        if (!id) {
            return null;
        }

        // TODO: is there a better/more reliable way to do this?
        var slashIndex = id.lastIndexOf("/");
        if (slashIndex >= 1) {
            return id.substring(0, 1).toUpperCase() + id.substring(1, slashIndex);
        }

        return id;
    }
}

export = document;