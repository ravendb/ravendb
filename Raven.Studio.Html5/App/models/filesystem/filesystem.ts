import resource = require("models/resources/resource");
import license = require("models/auth/license");
import fileSystemStatistics = require("models/filesystem/fileSystemStatistics");

class filesystem extends resource {
    statistics = ko.observable<fileSystemStatistics>();
    files = ko.observableArray<filesystemFileHeaderDto>();
    static type = "filesystem";

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false, bundles: string[] = []) {
        super(name, filesystem.type, isAdminCurrentTenant);
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().fileCountText() : "0 files");
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var ravenFsValue = license.licenseStatus().Attributes.ravenfs;
                return /^true$/i.test(ravenFsValue);
            }
            return true;
        });
    }

    activate() {
        ko.postbox.publish("ActivateFilesystem", this);
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("filesystems/");
        return (index > 0) ? url.substring(index + 10) : "";
    }

    saveStatistics(dto: filesystemStatisticsDto) {
        this.statistics(new fileSystemStatistics(dto));
    }
}
export = filesystem;