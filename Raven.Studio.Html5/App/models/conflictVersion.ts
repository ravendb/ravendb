import document = require("models/document");
import appUrl = require("common/appUrl");
import database = require("models/database");

class conflictVersion {
    id: string;
    sourceId: string;

    constructor(dto: conflictVersionsDto) {
        this.id = dto.Id;
        this.sourceId = dto.SourceId;
    }
}

export = conflictVersion;