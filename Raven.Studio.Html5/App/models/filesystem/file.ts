import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id: string;
    Size: string;
    LastModified: string;
    directory: string;

    __metadata: fileMetadata;

    constructor(dto?: filesystemFileHeaderDto, excludeDirectoryInId?: boolean) {
        if (dto) {
            if (dto.FullPath && dto.FullPath[0] === "/") {
                dto.FullPath = dto.FullPath.replace("/", "");
            }

            if (excludeDirectoryInId) {
                this.id = dto.FullPath.substring(dto.FullPath.lastIndexOf("/") + 1);
                this.directory = dto.FullPath.substring(0, dto.FullPath.lastIndexOf("/"));
            }
            else {
                this.id = dto.FullPath;
            }
            if (dto.HumaneTotalSize === " Bytes") {
                dto.HumaneTotalSize = "0 Bytes";
            }
            this.Size = dto.HumaneTotalSize;
            this.LastModified = dto.Metadata["Last-Modified"];

            this.__metadata = new fileMetadata(dto.Metadata);
        }
    }

    getId() {
        return this.id;
    }

    getUrl() {
        return this.directory ? this.directory + "/" +this.id : this.id;
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Size", "LastModified"];
    }

}

export = file;