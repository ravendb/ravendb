class conflictVersion {
    id: string;
    sourceId: string;

    constructor(dto: conflictVersionsDto) {
        this.id = dto.Id;
        this.sourceId = dto.SourceId;
    }
}

export = conflictVersion;