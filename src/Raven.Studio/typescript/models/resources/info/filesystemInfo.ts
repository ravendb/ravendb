/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import filesystem = require("models/filesystem/filesystem");
import resourcesManager = require("common/shell/resourcesManager");

class filesystemInfo extends resourceInfo {

    filesCount = ko.observable<number>();

    constructor(dto: Raven.Client.Data.FileSystemInfo) {
        super(dto);
        this.update(dto);
    }

    get qualifier() {
        return "fs";
    }

    get fullTypeName() {
        return "filesystem";
    }

    asResource(): filesystem {
        return resourcesManager.default.getFileSystemByName(this.name);
    }
    update(fileSystemInfo: Raven.Client.Data.FileSystemInfo): void {
        super.update(fileSystemInfo);

        this.filesCount(fileSystemInfo.FilesCount);
    }
}

export = filesystemInfo;
