import resource = require("models/resources/resource");
import license = require("models/auth/license");
import fileSystemStatistics = require("models/filesystem/fileSystemStatistics");

class filesystem extends resource {
    activeBundles = ko.observableArray<string>();
    isImporting = ko.observable<boolean>(false);
    importStatus = ko.observable<string>("");
    statistics = ko.observable<fileSystemStatistics>();
    files = ko.observableArray<filesystemFileHeaderDto>();
    static type = 'filesystem';

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false, bundles: string[] = null) {
        super(name, filesystem.type, isAdminCurrentTenant);
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().fileCount() : 0);
        this.itemCountText = ko.computed(() => {
            var itemCount = this.itemCount();
            var text = itemCount + ' file';
            if (itemCount != 1) {
                text += 's';
            }
            return text;
        });
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