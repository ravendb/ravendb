class synchronizationDetail implements documentBase {

    fileName: string;
    fileETag: string;
    DestinationUrl: string;
    Type: filesystemSynchronizationType;
    Status: string;

    constructor(dto: filesystemSynchronizationDetailsDto, status?: string) {
        this.fileName = dto.FileName;
        this.fileETag = dto.FileETag;
        this.DestinationUrl = dto.DestinationUrl;
        this.Type = dto.Type;
        this.Status = status;
    }

    getId() {
        return this.fileName;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "DestinationUrl", "Type", "Status"];
    }
}

export = synchronizationDetail;

