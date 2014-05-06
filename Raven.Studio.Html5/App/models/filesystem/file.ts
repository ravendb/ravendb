import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id: string;
    Size: string;
    LastModified: string;
    __metadata: fileMetadata;

    constructor(dto?: filesystemFileHeaderDto) {
        if (dto) {
            this.id = dto.Name;
            this.Size = dto.HumaneTotalSize;
            this.LastModified = dto.Metadata["Last-Modified"];

            this.__metadata = new fileMetadata(dto.Metadata);
        }
    }

    getId() {
        return this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "Size", "LastModified"];
    }

}

export = file;