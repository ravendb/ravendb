import resource = require("models/resource");

class database extends resource {
    statistics = ko.observable<databaseStatisticsDto>();
    activeBundles = ko.observableArray<string>();
    isImporting = ko.observable<boolean>(false);
    importStatus = ko.observable<string>('');
    static type = 'database';

    constructor(public name: string, isDisabled: boolean = false, bundles: Array<string> = []) {
        super(name, database.type);
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
        this.itemCountText = ko.computed(() => {
            var itemCount = this.itemCount();
            var text = itemCount + ' document';
            if (itemCount != 1) {
                text = text + 's';
            }
            return text;
        });
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