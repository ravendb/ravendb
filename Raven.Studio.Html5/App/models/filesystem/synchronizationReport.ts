class synchronizationReport implements documentBase {

    FileName: string;
    fileETag: string;
    Type: filesystemSynchronizationType;

    bytesTransfered: number;
    bytesCopied: number;
    needListLength: number;

    exception: any;

    constructor(dto: filesystemSynchronizationReportDto) {
        this.FileName = dto.FileName;
        this.fileETag = dto.FileETag;
        this.Type = dto.Type;

        this.bytesTransfered = dto.BytesTransfered;
        this.bytesCopied = dto.BytesCopied;
        this.needListLength = dto.NeedListLength;

        this.exception = dto.Exception;
    }

    getId() {
        return this.FileName;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "Type"];
    }
}

export = synchronizationReport;