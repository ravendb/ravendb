import resource = require("models/resource");
import license = require("models/license");

class database extends resource {
    statistics = ko.observable<databaseStatisticsDto>();
    activeBundles = ko.observableArray<string>();
    isImporting = ko.observable<boolean>(false);
    importStatus = ko.observable<string>('');
    indexingDisabled = ko.observable<boolean>(false);
    rejectClientsMode = ko.observable<boolean>(false);
    recentQueriesLocalStorageName: string;
    mergedIndexLocalStoragePrefix: string;
    static type = 'database';

    constructor(public name: string, isDisabled: boolean = false, bundles: Array<string> = [], isIndexingDisabled: boolean = false, isRejectClientsMode = false) {
        super(name, database.type);
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.indexingDisabled(isIndexingDisabled);
        this.rejectClientsMode(isRejectClientsMode);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
        this.itemCountText = ko.computed(() => {
            var itemCount = this.itemCount();
            var text = itemCount.toLocaleString() + ' document';
            if (itemCount != 1) {
                text += 's';
            }
            return text;
        });
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var attributes = license.licenseStatus().Attributes;
                var result = this.activeBundles()
                    .map(bundleName => this.attributeValue(attributes, bundleName === "periodicBackup" ? "periodicExport" : bundleName))
                    .reduce((a, b) => /^true$/i.test(a) && /^true$/i.test(b), true);
                return result;
            }
            return true;
        });
        this.recentQueriesLocalStorageName = 'ravenDB-recentQueries.' + name;
        this.mergedIndexLocalStoragePrefix = 'ravenDB-mergedIndex.' + name;
    }

    private attributeValue(attributes, bundleName: string) {
        for (var key in attributes){
            if (attributes.hasOwnProperty(key) && key.toLowerCase() === bundleName.toLowerCase()) {
                return attributes[key];
            }
        }
        return "true";
    }

	activate() {
        ko.postbox.publish("ActivateDatabase", this);
	}

    static getNameFromUrl(url: string) {
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}

export = database;