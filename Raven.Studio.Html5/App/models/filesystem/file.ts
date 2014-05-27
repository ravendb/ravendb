import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id: string;
    Size: string;
    LastModified: string;
    directory: string;
    __metadata: fileMetadata;

    constructor(dto?: filesystemFileHeaderDto, excludeDirectoryInId?: boolean) {
        if (dto) {
            if (excludeDirectoryInId) {
                this.id = dto.Name.substring(dto.Name.lastIndexOf("/") + 1);
            }
            else {
                this.id = dto.Name;
            }
            this.directory = dto.Name.substring(0, dto.Name.lastIndexOf("/"))
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