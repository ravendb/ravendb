/// <reference path="../../../../typings/tsd.d.ts"/>

import conflictVersion = require("models/database/replication/conflictVersion");

class conflict {
    id: string;
    conflictDetectedAt: string;
    versions: conflictVersion[];

    constructor(dto: conflictDto) {
        this.id = dto.Id;
        this.conflictDetectedAt = dto.ConflictDetectedAt;
        this.versions = $.map(dto.Versions, v => new conflictVersion(v));
    }

    getId() {
        return this.id;
    }

    getUrl() {
        return this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "conflictDetectedAt", "versions"];
    }

}

export = conflict;
