/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import filesystem = require("models/filesystem/filesystem");

class filesystemInfo extends resourceInfo {

    constructor(dto: Raven.Client.Data.FileSystemInfo) {
        super(dto);
    }

    get qualifier() {
        return "fs";
    }

    get fullTypeName() {
        return "filesystem";
    }

    asResource(): filesystem {
        return new filesystem(this.name);
    }
}

export = filesystemInfo;
