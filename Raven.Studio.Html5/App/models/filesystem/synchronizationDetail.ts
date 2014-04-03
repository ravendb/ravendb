



class synchronizationDetail {

    filename: string;
    fileETag: string;
    destinationUrl: string;
    type: filesystemSynchronizationType;

    constructor(dto: filesystemSynchronizationDetailsDto) {
        this.filename = dto.Filename;
        this.fileETag = dto.FileETag;
        this.destinationUrl = dto.DestinationUrl;
        this.type = dto.Type;
    }
}

export = synchronizationDetail;

