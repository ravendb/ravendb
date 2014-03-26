import document = require("models/document");
import database = require("models/database");

class replicationSource {
    lastDocumentEtag: string;
    lastAttachmentEtag: string;
    serverInstanceId: string;
    source: string;
    name: string;

    constructor(dto: replicationSourceDto) {
        this.lastAttachmentEtag = dto.LastAttachmentEtag;
        this.lastDocumentEtag = dto.LastDocumentEtag;
        this.serverInstanceId = dto.ServerInstanceId;
        this.source = dto.Source;
        this.name = database.getNameFromUrl(dto.Source);
    }
}

export = replicationSource;