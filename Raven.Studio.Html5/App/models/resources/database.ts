import resource = require("models/resources/resource");
import license = require("models/auth/license");
import databaseStatistics = require("models/resources/databaseStatistics");

class database extends resource {
    statistics = ko.observable<databaseStatistics>();
    indexingDisabled = ko.observable<boolean>(false);
    rejectClientsMode = ko.observable<boolean>(false);
    clusterWide = ko.observable<boolean>(false);
    recentQueriesLocalStorageName: string;
    recentPatchesLocalStorageName: string;
    mergedIndexLocalStoragePrefix: string;
    static type = "database";
    iconName: KnockoutComputed<string>;

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false, bundles: string[] = [], isIndexingDisabled: boolean = false, isRejectClientsMode = false, isLoaded = false, clusterWide = false) {
        super(name, TenantType.Database, isAdminCurrentTenant);
        this.fullTypeName = "Database";
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.indexingDisabled(isIndexingDisabled);
        this.rejectClientsMode(isRejectClientsMode);
        this.isLoaded(isLoaded);
        this.clusterWide(clusterWide);
        this.iconName = ko.computed(() => !this.clusterWide() ? "fa fa-fw fa-database" : "fa fa-fw fa-cubes");
        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().countOfDocumentsText() : "");
        this.isLicensed = ko.pureComputed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var attributes = license.licenseStatus().Attributes;
                var result = this.activeBundles()
                    .map(bundleName => this.attributeValue(attributes, bundleName === "periodicBackup" ? "periodicExport" : bundleName))
                    .reduce((a, b) => /^true$/i.test(a) && /^true$/i.test(b), true);
                return result;
            }
            return true;
        });
        this.recentQueriesLocalStorageName = "ravenDB-recentQueries." + name;
        this.recentPatchesLocalStorageName = "ravenDB-recentPatches." + name;
        this.mergedIndexLocalStoragePrefix = "ravenDB-mergedIndex." + name;
    }

    activate() {
        this.isLoaded(true);
        ko.postbox.publish("ActivateDatabase", this);
    }

    saveStatistics(dto: reducedDatabaseStatisticsDto) {
        if (!this.statistics()) {
            this.statistics(new databaseStatistics());
        }

        this.statistics().fromDto(dto);
    }

    private attributeValue(attributes, bundleName: string) {
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

    isBundleActive(bundleName: string) {
        if (!!bundleName) {
            var bundle = this.activeBundles.first((x: string) => x.toLowerCase() === bundleName.toLowerCase());
            return !!bundle;
        }
        return false;
    }
}

export = database;
