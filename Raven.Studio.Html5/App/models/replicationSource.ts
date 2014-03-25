import document = require("models/document");
import database = require("models/database");

class replicationSource extends document {
    lastDocumentEtag: string;
    lastAttachmentEtag: string;
    serverInstanceId: string;
    source: string;
    name: string;

    constructor(dto: replicationSourceDto) {
        super(dto);
        this.lastAttachmentEtag = dto.LastAttachmentEtag;
        this.lastDocumentEtag = dto.LastDocumentEtag;
        this.serverInstanceId = dto.ServerInstanceId;
        this.source = dto.Source;
        this.name = database.getNameFromUrl(dto.Source);
    }

    getEntityName() {
        return null;
    }

}

export = replicationSource;