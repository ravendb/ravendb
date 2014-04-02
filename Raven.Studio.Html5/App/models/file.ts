import fileMetadata = require("models/fileMetadata");

class file implements documentBase {
    id: string;
    Size: string;
    LastModified: string;
    __metadata: fileMetadata;

    constructor(dto: filesystemFileHeaderDto) {
        this.id = dto.Name;
        this.Size = dto.HumaneTotalSize;
        this.LastModified = dto.Metadata["Last-Modified"];

        this.__metadata = new fileMetadata(dto.Metadata);
    }

    getId() {
        return this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "Size", "LastModified"];
    }

}

export = file;