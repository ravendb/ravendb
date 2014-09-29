import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id: string;
    size: string;
    lastModified: Date;
    directory: string;
    fullPath: string;


    __metadata: fileMetadata;

    constructor(dto?: filesystemFileHeaderDto, excludeDirectoryInId?: boolean) {
        if (dto) {           

            if (excludeDirectoryInId) {
                this.id = dto.Name
            }
            else {
                this.id = dto.FullPath;
            }
            if (dto.HumaneTotalSize === " Bytes") {
                dto.HumaneTotalSize = "0 Bytes";
            }

            this.directory = dto.Directory;
            this.fullPath = dto.FullPath;
            this.size = dto.HumaneTotalSize;            
            this.lastModified = dto.LastModified;

            this.__metadata = new fileMetadata(dto.Metadata);
        }
    }

    getId() {
        return this.id;
    }

    getUrl() {
        return this.fullPath ? this.fullPath : this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["size", "lastModified"];
    }

}

export = file;