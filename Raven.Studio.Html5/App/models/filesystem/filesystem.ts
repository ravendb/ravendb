import resource = require("models/resources/resource");
import license = require("models/auth/license");
import fileSystemStatistics = require("models/filesystem/fileSystemStatistics");

class filesystem extends resource {
    statistics = ko.observable<fileSystemStatistics>();
    files = ko.observableArray<filesystemFileHeaderDto>();
    static type = "filesystem";
    iconName = "fa fa-fw fa-file-image-o";

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false,
        isLoaded: boolean = false, bundles: string[] = [], stats: filesystemStatisticsDto = null) {
        super(name, TenantType.FileSystem, isAdminCurrentTenant);
        this.fullTypeName = "File System";
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.isLoaded(isLoaded);
        if (!!stats) {
            this.saveStatistics(stats);
        }

        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().fileCountText() : "");
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var ravenFsValue = license.licenseStatus().Attributes.ravenfs;
                return /^true$/i.test(ravenFsValue);
            }
            return true;
        });
    }

    activate() {
        this.isLoaded(true);
        ko.postbox.publish("ActivateFilesystem", this);
    }

    saveStatistics(dto: filesystemStatisticsDto) {
        if (!this.statistics()) {
            this.statistics(new fileSystemStatistics());
        }

        this.statistics().fromDto(dto);
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("filesystems/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}
export = filesystem;
