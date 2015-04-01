class replicationConfig {

    private static DO_NOT_RESOLVE_AUTOMATICALLY = "None";
    private static RESOLVE_TO_LOCAL = "ResolveToLocal";
    private static RESOLVE_TO_REMOTE = "ResolveToRemote";
    private static RESOLVE_TO_LATEST = "ResolveToLatest";

    documentConflictResolution = ko.observable<string>().extend({ required: true });
    attachmentConflictResolution = ko.observable<string>().extend({ required: true });

    constructor(dto: replicationConfigDto) {
        this.documentConflictResolution(dto.DocumentConflictResolution);
        this.attachmentConflictResolution(dto.AttachmentConflictResolution);

        this.documentConflictResolution.subscribe((val) => this.attachmentConflictResolution(val)); // todo: remove that if decided to treat attachments differently

    }

    toDto(): replicationConfigDto {
        return {
            DocumentConflictResolution: this.documentConflictResolution(),
            AttachmentConflictResolution: this.attachmentConflictResolution()
        };
    }

    clear() {
        this.documentConflictResolution("None");
        this.attachmentConflictResolution("None");
    }
}

export = replicationConfig;