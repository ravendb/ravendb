/// <reference path="../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");

class database extends resource {
    //TODO: remove local storage name props from this class
    recentQueriesLocalStorageName: string;
    recentPatchesLocalStorageName: string;
    mergedIndexLocalStoragePrefix: string;
    starredDocumentsLocalStorageName: string;

    static readonly type = "database";
    static readonly qualifier = "db";

    constructor(name: string, isAdminCurrentTenant: boolean, bundles: string[]) {
        super(name, isAdminCurrentTenant, bundles);
        /* TODO
        this.isLicensed = ko.pureComputed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var attributes = license.licenseStatus().Attributes;
                var result = this.activeBundles()
                    .map(bundleName => this.attributeValue(attributes, bundleName === "periodicBackup" ? "periodicExport" : bundleName))
                    .reduce((a, b) => /^true$/i.test(a) && /^true$/i.test(b), true);
                return result;
            }
            return true;
        });*/
        this.recentQueriesLocalStorageName = "ravenDB-recentQueries." + name;
        this.recentPatchesLocalStorageName = "ravenDB-recentPatches." + name;
        this.mergedIndexLocalStoragePrefix = "ravenDB-mergedIndex." + name;
        this.starredDocumentsLocalStorageName = "ravenDB-starredDocuments." + name;
    }

    private attributeValue(attributes: any, bundleName: string) {
        for (var key in attributes){
            if (attributes.hasOwnProperty(key) && key.toLowerCase() === bundleName.toLowerCase()) {
                return attributes[key];
            }
        }
        return "true";
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }

    isBundleActive(bundleName: string): boolean {
        if (bundleName) {
            return !!this.activeBundles().find((x: string) => x.toLowerCase() === bundleName.toLowerCase());
        }
        return false;
    }

    get fullTypeName() {
        return "Database";
    }

    get qualifier() {
        return database.qualifier;
    }

    get urlPrefix() {
        return "databases";
    }

    get type() {
        return database.type;
    }

    updateUsing(incomingCopy: this): void {
        super.updateUsing(incomingCopy);

        //TODO: assign other props
    }
}

export = database;
