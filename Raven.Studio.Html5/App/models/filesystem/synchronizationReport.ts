

class synchronizationReport {

    fileName: string;
    fileETag: string;
    type: filesystemSynchronizationType;

    bytesTransfered: number;
    bytesCopied: number;
    needListLength: number;

    exception: any;

    constructor(dto: filesystemSynchronizationReportDto) {
        this.fileName = dto.FileName;
        this.fileETag = dto.FileETag;
        this.type = dto.Type;

        this.bytesTransfered = dto.BytesTransfered;
        this.bytesCopied = dto.BytesCopied;
        this.needListLength = dto.NeedListLength;

        this.exception = dto.Exception;
    }
}

export = synchronizationReport;