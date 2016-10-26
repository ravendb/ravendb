/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import filesystem = require("models/filesystem/filesystem");

class filesystemInfo extends resourceInfo {

    filesCount = ko.observable<number>();

    constructor(dto: Raven.Client.Data.FileSystemInfo) {
        super(dto);
        this.filesCount(dto.FilesCount);
    }

    get qualifier() {
        return "fs";
    }

    get fullTypeName() {
        return "filesystem";
    }

    asResource(): filesystem {
        return new filesystem(this.name, this.isAdmin(), this.disabled(), this.bundles());
    }
}

export = filesystemInfo;
