/// <reference path="../../../../typings/tsd.d.ts"/>

class replicationConfig {

    private static DO_NOT_RESOLVE_AUTOMATICALLY = "None";

    private static RESOLVE_TO_LOCAL = "ResolveToLocal";
    private static RESOLVE_TO_REMOTE = "ResolveToRemote";
    private static RESOLVE_TO_LATEST = "ResolveToLatest";

    documentConflictResolution = ko.observable<string>().extend({ required: true });

    constructor(dto: replicationConfigDto) {
        this.documentConflictResolution(dto.DocumentConflictResolution);
    }

    toDto(): replicationConfigDto {
        return {
            DocumentConflictResolution: this.documentConflictResolution(),
        };
    }

    clear() {
        this.documentConflictResolution("None");

    }
}

export = replicationConfig;
