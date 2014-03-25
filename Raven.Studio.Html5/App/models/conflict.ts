import document = require("models/document");
import conflictVersion = require("models/conflictVersion");

class conflict extends document {
    id: string;
    conflictDetectedAt: string;
    versions: conflictVersion[];

    constructor(dto: conflictDto) {
        super(dto);
        this.id = dto.Id;
        this.conflictDetectedAt = dto.ConflictDetectedAt;
        this.versions = $.map(dto.Versions, v => new conflictVersion(v));
    }

    getEntityName() {
        return null;
    }

    getId() {
        return this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "conflictDetectedAt", "versions"];
    }

}

export = conflict;