import resource = require("models/resource");

class database extends resource {
    statistics = ko.observable<databaseStatisticsDto>();
    activeBundles = ko.observableArray<string>();

    constructor(public name: string, isDisabled: boolean = false, private bundles = []) {
        super(name, 'database');
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