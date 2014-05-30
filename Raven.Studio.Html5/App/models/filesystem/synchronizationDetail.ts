class synchronizationDetail implements documentBase {

    fileName: string;
    fileETag: string;
    DestinationUrl: string;
    Type: filesystemSynchronizationType;
    TypeDescription: string;
    Status: string;   

    constructor(dto: filesystemSynchronizationDetailsDto, status?: string) {
        this.fileName = dto.FileName;
        this.fileETag = dto.FileETag;
        this.DestinationUrl = dto.DestinationUrl;
        this.Type = dto.Type;
        this.TypeDescription = synchronizationDetail.getTypeDescription(dto.Type);
        this.Status = status;
    }

    getId() {
        return this.fileName;
    }

    getUrl() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "DestinationUrl", "Type", "Status"];
    }

    static getTypeDescription(type: filesystemSynchronizationType) {
        switch (type) {
            case filesystemSynchronizationType.ContentUpdate:
                return "Content Update";
            case filesystemSynchronizationType.Delete:
                return "Delete";
            case filesystemSynchronizationType.MetadataUpdate:
                return "Metadata Update";
            case filesystemSynchronizationType.Rename:
                return "Rename";
            default:
                return "Unknown";
        }
    }
}

export = synchronizationDetail;

