class replicationConfig {

    private static DO_NOT_RESOLVE_AUTOMATICALLY = "None";
    private static RESOLVE_TO_LOCAL = "ResolveToLocal";
    private static RESOLVE_TO_REMOTE = "ResolveToRemote";

    documentConflictResolution = ko.observable<string>().extend({ required: true });
    attachmentConflictResolution = ko.observable<string>().extend({ required: true });

    constructor(dto: replicationConfigDto) {
        this.documentConflictResolution(dto.DocumentConflictResolution);
        this.attachmentConflictResolution(dto.AttachmentConflictResolution);
    }

    toDto(): replicationConfigDto {
        return {
            DocumentConflictResolution: this.documentConflictResolution(),
            AttachmentConflictResolution: this.attachmentConflictResolution()
        };
    }

    resolveDocumentsConflictsToLocal() {
        this.documentConflictResolution(replicationConfig.RESOLVE_TO_LOCAL);
    }

    resolveDocumentsConflictsToRemote() {
        this.documentConflictResolution(replicationConfig.RESOLVE_TO_REMOTE);
    }

    doNotResolveDocumentsConflictsAutomatically() {
        this.documentConflictResolution(replicationConfig.DO_NOT_RESOLVE_AUTOMATICALLY);
    }

    resolveAttachmentsConflictsToLocal() {
        this.attachmentConflictResolution(replicationConfig.RESOLVE_TO_LOCAL);
    }

    resolveAttachmentsConflictsToRemote() {
        this.attachmentConflictResolution(replicationConfig.RESOLVE_TO_REMOTE);
    }

    doNotResolveAttachmentsConflictsAutomatically() {
        this.attachmentConflictResolution(replicationConfig.DO_NOT_RESOLVE_AUTOMATICALLY);
    }
}

export = replicationConfig;