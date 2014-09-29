import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id: string;
    Size: string;
    LastModified: string;
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
            this.Size = dto.HumaneTotalSize;            
            this.LastModified = moment(dto.LastModified).format();

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
        return ["Size", "LastModified"];
    }

}

export = file;