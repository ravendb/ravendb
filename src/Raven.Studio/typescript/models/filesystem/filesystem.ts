/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");
import resource = require("models/resources/resource");
import license = require("models/auth/license");

class filesystem extends resource {
    static readonly type = "filesystem";
    static readonly qualifier = "fs";

    constructor(name: string, isAdminCurrentTenant: boolean = true, bundles: string[] = []) {
        super(name, isAdminCurrentTenant, bundles);
        /* TODO: move somewhere else 
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var ravenFsValue = license.licenseStatus().Attributes.ravenfs;
                return /^true$/i.test(ravenFsValue);
            }
            return true;
        });*/
    }

    get qualifier() {
        return filesystem.qualifier;
    }

    get urlPrefix() {
        return "fs";
    }

    get fullTypeName() {
        return "File System";
    }

    get type() {
        return filesystem.type;
    }

    static getNameFromUrl(url: string) {
        const index = url.indexOf("filesystems/");
        return (index > 0) ? url.substring(index + 10) : "";
    }

    //TODO: update using ?
}
export = filesystem;
