/// <reference path="../../../typings/tsd.d.ts"/>

import fileMetadata = require("models/filesystem/fileMetadata");

class file implements documentBase {
    id = ko.observable<string>();
    Size: string;
    LastModified: Date;
    directory: string;
    fullPath: string;

    __metadata: fileMetadata;

    constructor(dto?: filesystemFileHeaderDto, excludeDirectoryInId?: boolean) {
        if (dto) {
            if (dto.FullPath && dto.FullPath[0] === "/") {
                dto.FullPath = dto.FullPath.replace("/", "");
            }
            if (excludeDirectoryInId) {
                this.id(dto.Name);
            }
            else {
                this.id(dto.FullPath);
            }
            if (dto.HumaneTotalSize === " Bytes") {
                dto.HumaneTotalSize = "0 Bytes";
            }

            this.directory = dto.Directory;
            this.fullPath = dto.FullPath;
            this.Size = dto.HumaneTotalSize;
            this.LastModified = dto.Metadata["Last-Modified"];

            this.__metadata = new fileMetadata(dto.Metadata);
        }
    }

    getId() {
        return this.id();
    }

    setId(newName: string) {
        this.id(newName);
    }

    getUrl() {
        return this.fullPath ? this.fullPath : this.id();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Size", "LastModified"];
    }
}

export = file;
