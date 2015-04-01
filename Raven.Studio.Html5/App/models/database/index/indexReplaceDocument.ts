import document = require("models/database/documents/document");

class indexReplaceDocument extends document {

    static replaceDocumentPrefix = "Raven/Indexes/Replace/";

    indexToReplace: string;
    minimumEtagBeforeReplace: string;
    replaceTimeUtc: string;

    constructor(dto: indexReplaceDocumentDto) {
        super(dto);
        this.indexToReplace = dto.IndexToReplace;
        this.minimumEtagBeforeReplace = dto.MinimumEtagBeforeReplace;
        this.replaceTimeUtc = dto.ReplaceTimeUtc;
    }

    toDto(): indexReplaceDocumentDto {
        var meta = this.__metadata.toDto();
        return {
            '@metadata': meta,
            IndexToReplace: this.indexToReplace,
            MinimumEtagBeforeReplace: this.minimumEtagBeforeReplace,
            ReplaceTimeUtc: this.replaceTimeUtc
        };
    }

    extractReplaceWithIndexName() {
        var fullKey = this.getId();
        return fullKey.substr(indexReplaceDocument.replaceDocumentPrefix.length);
    }
}

export = indexReplaceDocument;